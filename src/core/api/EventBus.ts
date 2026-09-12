/*
 *
 * Copyright 2008-2021 Kinotic and the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {ConnectionInfo, ServerInfo} from '@/api/ConnectionInfo'
import {AuthenticationError} from '@/api/errors/AuthenticationError'
import {ConnectionLostError} from '@/api/errors/ConnectionLostError'
import {ContinuumError} from '@/api/errors/ContinuumError'
import {ConnectedInfo} from '@/api/security/ConnectedInfo'
import {StompConnectionManager} from '@/core/api/StompConnectionManager'
import {context, propagation} from '@opentelemetry/api';
import {IFrame, IMessage} from '@stomp/rx-stomp'
import {ConnectableObservable, firstValueFrom, Observable, Subject, Subscription, throwError, Unsubscribable} from 'rxjs'
import {filter, map, multicast} from 'rxjs/operators'
import {Optional} from 'typescript-optional'
import {v4 as uuidv4} from 'uuid'
import {EventConstants, IEvent, IEventBus} from './IEventBus'

/**
 * Default IEvent implementation
 */
export class Event implements IEvent {

    public cri: string
    public headers: Map<string, string>
    public data: Optional<Uint8Array>

    constructor(cri: string,
                headers?: Map<string, string>,
                data?: Uint8Array) {

        this.cri = cri

        if (headers !== undefined) {
            this.headers = headers
        } else {
            this.headers = new Map<string, string>()
        }

        this.data = Optional.ofNullable(data)
    }

    public getHeader(key: string): string | undefined {
        return this.headers.get(key)
    }

    public hasHeader(key: string): boolean {
        return this.headers.has(key)
    }

    public setHeader(key: string, value: string): void {
        this.headers.set(key, value)
    }

    public removeHeader(key: string): boolean {
        return this.headers.delete(key)
    }

    public setDataString(data: string): void {
        const uint8Array = new TextEncoder().encode(data)
        this.data = Optional.ofNonNull(uint8Array)
    }

    public getDataString(): string {
        let ret = ''
        this.data.ifPresent(( value ) => ret = new TextDecoder().decode(value))
        return ret
    }
}

interface Carrier {
    traceparent?: string;
    tracestate?: string;
}

/**
 * Default implementation of {@link IEventBus}
 */
export class EventBus implements IEventBus {

    /**
     * Emits when the connection has been torn down by something other than a call to disconnect:
     * the server refused a connect or a reconnect. By then the connection is already deactivated and
     * every pending request has been failed; what remains is the caller's decision on whether and how
     * to connect again. This is hot - it emits whether or not anyone is subscribed - so a subscription
     * added after the fact will not replay a loss that has already happened.
     */
    public readonly fatalErrors: Observable<ContinuumError>
    public serverInfo: ServerInfo | null = null
    private stompConnectionManager: StompConnectionManager = new StompConnectionManager()
    private replyToCri: string  | null = null
    private requestRepliesObservable: ConnectableObservable<IEvent> | null = null
    private requestRepliesSubject: Subject<IEvent> | null = null
    private requestRepliesSubscription: Subscription | null = null
    private fatalErrorSubject: Subject<ContinuumError> = new Subject<ContinuumError>()

    constructor() {
        this.fatalErrors = this.fatalErrorSubject.asObservable()
        // The manager tears the connection down on a refused connect, and when the caller's bound on
        // reconnect attempts is exhausted; this runs as part of both. The refusal is reported from the
        // ERROR frame itself, so only the exhausted bound needs reporting here - once the initial connect
        // has succeeded there is no promise left to reject, and without this it would end in silence.
        this.stompConnectionManager.deactivationHandler = () => {
            const manager = this.stompConnectionManager
            const refusal = manager.lastRefusal
            const attemptsExhausted = manager.maxConnectionAttemptsReached
            // Only a loss after a working connection is reported here. A refused initial connect
            // already reaches the caller as the rejected connect() promise, exactly as before.
            const report = manager.initialConnectionSucceeded
            this.cleanup()
            if (!report) {
                return
            }
            if (refusal) {
                this.fatalErrorSubject.next(EventBus.toFatalError(refusal))
            } else if (attemptsExhausted) {
                this.fatalErrorSubject.next(new ContinuumError(
                    'Max connection attempts reached; the connection has been given up on'))
            }
        }
        // The socket dropping fails what was in flight. Reconnection, if any, carries on underneath
        this.stompConnectionManager.connectionLostHandler = () => {
            this.failPendingRequests(new ConnectionLostError(
                'Connection to the server was lost while this request was in flight; it will not receive a reply'))
        }
    }

    public isConnectionActive(): boolean{
        return this.stompConnectionManager.active
    }

    public isConnected(): boolean {
        return this.stompConnectionManager.connected
    }

    public async connect(connectionInfo: ConnectionInfo): Promise<ConnectedInfo> {
        if(!this.stompConnectionManager.active){

            // reset state in case connection ended due to max connection attempts
            this.cleanup()

            const connectedInfo = await this.stompConnectionManager.activate(connectionInfo)
            // manually copy so we don't store any sensitive info
            this.serverInfo = new ServerInfo()
            this.serverInfo.host = connectionInfo.host
            this.serverInfo.port = connectionInfo.port
            this.serverInfo.useSSL = connectionInfo.useSSL

            // FIXME: a reply should not need a reply, therefore a replyCri probably should not be a EventConstants.SERVICE_DESTINATION_PREFIX
            this.replyToCri = this.stompConnectionManager.replyToCri


            return connectedInfo
        }else{
            throw new Error('Event Bus connection already active')
        }
    }

    public async disconnect(force?: boolean): Promise<void> {
        await this.stompConnectionManager.deactivate(force)

        this.cleanup()
    }

    public send(event: IEvent): void {
        if(this.stompConnectionManager.rxStomp){
            const headers: any = {}

            for (const [key, value] of event.headers.entries()) {
                headers[key] = value
            }

            const carrier: Carrier = {}
            propagation.inject(context.active(), carrier)
            if(carrier.traceparent){
                headers[EventConstants.TRACEPARENT_HEADER] = carrier.traceparent
            }
            if(carrier.tracestate){
                headers[EventConstants.TRACESTATE_HEADER] = carrier.tracestate
            }

            // send data over stomp
            this.stompConnectionManager.rxStomp.publish({
                                                            destination: event.cri,
                                                            headers,
                                                            binaryBody: event.data.orUndefined()
                                                        })
        }else{
            throw this.createSendUnavailableError()
        }
    }

    public request(event: IEvent): Promise<IEvent> {
        return firstValueFrom(this.requestStream(event, false))
    }

    public requestStream(event: IEvent, sendControlEvents: boolean = true): Observable<IEvent> {
        if(this.stompConnectionManager?.rxStomp){
            return new Observable<IEvent>((subscriber) => {

                if (this.requestRepliesObservable == null) {
                    this.requestRepliesSubject = new Subject<IEvent>()
                    this.requestRepliesObservable = this._observe(this.replyToCri as string)
                                                        .pipe(multicast(this.requestRepliesSubject)) as ConnectableObservable<IEvent>
                    this.requestRepliesSubscription = this.requestRepliesObservable.connect()
                }

                let serverSignaledCompletion = false
                const correlationId = uuidv4()
                const defaultMessagesSubscription: Unsubscribable
                          = this.requestRepliesObservable
                                .pipe(filter((value: IEvent): boolean => {
                                    return value.headers.get(EventConstants.CORRELATION_ID_HEADER) === correlationId
                                })).subscribe({
                                                  next(value: IEvent): void {

                                                      if (value.hasHeader(EventConstants.CONTROL_HEADER)) {

                                                          if (value.headers.get(EventConstants.CONTROL_HEADER) === 'complete') {
                                                              serverSignaledCompletion = true
                                                              subscriber.complete()
                                                          } else {
                                                              throw new Error('Control Header ' + value.headers.get(EventConstants.CONTROL_HEADER) + ' is not supported')
                                                          }

                                                      } else if (value.hasHeader(EventConstants.ERROR_HEADER)) {

                                                          // TODO: add custom error type that contains error detail as well if provided by server, this would be the event body
                                                          serverSignaledCompletion = true
                                                          subscriber.error(new Error(value.getHeader(EventConstants.ERROR_HEADER)))

                                                      } else {

                                                          subscriber.next(value)

                                                      }
                                                  },
                                                  error(err: any): void {
                                                      subscriber.error(err)
                                                  },
                                                  complete(): void {
                                                      subscriber.complete()
                                                  }
                                              })

                subscriber.add(defaultMessagesSubscription)

                event.setHeader(EventConstants.REPLY_TO_HEADER, this.replyToCri as string)
                event.setHeader(EventConstants.CORRELATION_ID_HEADER, correlationId)

                this.send(event)

                return () => {
                    if (sendControlEvents && !serverSignaledCompletion) {
                        // create control event to cancel long-running request
                        const controlEvent: Event = new Event(event.cri)
                        controlEvent.setHeader(EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_CANCEL)
                        controlEvent.setHeader(EventConstants.CORRELATION_ID_HEADER, correlationId)
                        this.send(controlEvent)
                    }
                }
            })
        }else{
            return throwError(() => this.createSendUnavailableError())
        }
    }

    public listen(_serverInfo: ServerInfo): Promise<void> {
        return Promise.reject('Not implemented')
    }

    public observe(cri: string): Observable<IEvent> {
        return this._observe(cri)
    }

    private cleanup(): void{
        this.failPendingRequests(new ConnectionLostError('Connection disconnected'))

        this.serverInfo = null
    }

    /**
     * Fails every request currently waiting on a reply and discards the reply subscription. The next
     * request recreates it on demand, so this is safe to call whether or not a reconnect follows.
     */
    private failPendingRequests(error: ConnectionLostError): void {
        if (this.requestRepliesSubject != null) {
            // Delivered to every caller waiting on an Event
            this.requestRepliesSubject.error(error)

            if (this.requestRepliesSubscription != null) {
                this.requestRepliesSubscription.unsubscribe()
                this.requestRepliesSubscription = null
            }

            this.requestRepliesSubject = null
            this.requestRepliesObservable = null
        }
    }

    private static toFatalError(frame: IFrame): ContinuumError {
        const message: string = frame.headers['message'] ?? 'Connection refused by server'
        // The gateway reports a refused credential or session as an authentication failure
        if (/authenticat/i.test(message)) {
            return new AuthenticationError(message)
        }
        return new ContinuumError(message)
    }

    /**
     * Creates the proper error to return if this.stompConnectionManager?.rxStomp is not available on a send request
     */
    private createSendUnavailableError(): Error {
        let ret: string = 'You must call connect on the event bus before sending any request'
        if(this.stompConnectionManager.maxConnectionAttemptsReached){
            ret = 'Max connection attempts reached event bus is not available'
        }
        return new Error(ret)
    }

    /**
     * This is internal impl of observe that creates a cold observable.
     * The public variants transform this to some type of hot observable depending on the need
     * @param cri to observe
     * @return the cold {@link Observable<IEvent>} for the given destination
     */
    private _observe(cri: string): Observable<IEvent> {
        if(this.stompConnectionManager?.rxStomp) {
            return this.stompConnectionManager
                       .rxStomp
                       .watch(cri)
                       .pipe(map<IMessage, IEvent>((message: IMessage): IEvent => {

                           // We translate all IMessage objects to IEvent objects
                           const headers: Map<string, string> = new Map<string, string>()
                           let destination: string = ''
                           for (const prop of Object.keys(message.headers)) {
                               if (prop === 'destination') {
                                   destination = message.headers[prop]
                               }else{
                                   headers.set(prop, message.headers[prop])
                               }
                           }

                           return new Event(destination, headers, message.binaryBody)
                       }))
        }else{
            throw this.createSendUnavailableError()
        }
    }

}

