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
import {ConnectionLostError} from '@/api/errors/ConnectionLostError'
import {ContinuumError} from '@/api/errors/ContinuumError'
import {ConnectedInfo} from '@/api/security/ConnectedInfo'
import {StompConnectionManager} from '@/core/api/StompConnectionManager'
import {context, propagation} from '@opentelemetry/api';
import {IMessage} from '@stomp/rx-stomp'
import {ConnectableObservable, firstValueFrom, Observable, Subject, Subscription, throwError, Unsubscribable} from 'rxjs'
import {filter, map, multicast, share} from 'rxjs/operators'
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
     * Emits when the connection was closed by something other than a call to disconnect(), after a
     * successful connect(): the server refused a reconnect, or the bound on reconnection attempts was
     * reached. By the time it emits the connection is already down and every pending request has been
     * failed; what remains is the caller's decision on whether and how to connect again, which it may
     * do from inside the subscription. A refusal before connect() resolves rejects that promise instead.
     * This is hot: a subscription added after the fact does not see a loss that has already happened.
     */
    public readonly fatalErrors: Observable<ContinuumError>
    public serverInfo: ServerInfo | null = null
    private stompConnectionManager: StompConnectionManager = new StompConnectionManager()
    private replyToCri: string  | null = null
    private requestRepliesObservable: ConnectableObservable<IEvent> | null = null
    private requestRepliesSubject: Subject<IEvent> | null = null
    private requestRepliesSubscription: Subscription | null = null
    private fatalErrorSubject: Subject<ContinuumError> = new Subject<ContinuumError>()
    /** Emits after each successful connect(), so what observes a destination can follow the connection */
    private readonly activated: Subject<void> = new Subject<void>()
    /** True from connect() resolving until the connection closes: what decides whether a loss is reported here or by connect() */
    private established: boolean = false
    /** Why the connection closed, if it closed on its own, so a send() into the dead connection can say so */
    private closeError: ContinuumError | null = null

    constructor() {
        this.fatalErrors = this.fatalErrorSubject.asObservable()
        this.stompConnectionManager.events.subscribe(event => {
            if (event.type === 'lost') {
                // Reconnection, if any, carries on underneath; what was in flight is not coming back
                this.failPendingRequests(new ConnectionLostError(
                    'Connection to the server was lost while this request was in flight; it will not receive a reply'))
            } else {
                // The error the connection closed with travels as the cause: one of these requests may
                // be the reason the server closed it, and a caller deciding whether to retry needs that
                this.failPendingRequests(new ConnectionLostError(
                    event.error ? `Connection closed: ${event.error.message}` : 'Connection disconnected',
                    event.error))
                this.serverInfo = null
                this.closeError = event.error ?? null
                if (event.error && this.established) {
                    this.fatalErrorSubject.next(event.error)
                }
                this.established = false
            }
        })
    }

    public isConnectionActive(): boolean{
        return this.stompConnectionManager.active
    }

    public isConnected(): boolean {
        return this.stompConnectionManager.connected
    }

    public async connect(connectionInfo: ConnectionInfo): Promise<ConnectedInfo> {
        if (this.stompConnectionManager.active) {
            throw new ContinuumError('Event Bus connection already active')
        }
        this.closeError = null

        const connectedInfo = await this.stompConnectionManager.activate(connectionInfo)
        this.established = true
        // manually copy so we don't store any sensitive info
        this.serverInfo = new ServerInfo()
        this.serverInfo.host = connectionInfo.host
        this.serverInfo.port = connectionInfo.port
        this.serverInfo.useSSL = connectionInfo.useSSL

        // FIXME: a reply should not need a reply, therefore a replyCri probably should not be a EventConstants.SERVICE_DESTINATION_PREFIX
        this.replyToCri = this.stompConnectionManager.replyToCri
        this.activated.next()

        return connectedInfo
    }

    public disconnect(force?: boolean): Promise<void> {
        // Everything else - failing what is pending, clearing serverInfo - happens on the closed event
        return this.stompConnectionManager.deactivate(force)
    }

    public send(event: IEvent): void {
        if (!this.stompConnectionManager.active) {
            throw this.createSendUnavailableError()
        }
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
        this.stompConnectionManager.publish({
                                                destination: event.cri,
                                                headers,
                                                binaryBody: event.data.orUndefined()
                                            })
    }

    public request(event: IEvent): Promise<IEvent> {
        return firstValueFrom(this.requestStream(event, false))
    }

    public requestStream(event: IEvent, sendControlEvents: boolean = true): Observable<IEvent> {
        if(!this.stompConnectionManager.active){
            return throwError(() => this.createSendUnavailableError())
        }
        return new Observable<IEvent>((subscriber) => {
            // Checked again here: an Observable made while connected may be subscribed after the
            // connection closed, and must not set up the reply address of a connection that is gone
            if (!this.stompConnectionManager.active) {
                subscriber.error(this.createSendUnavailableError())
                return
            }

            if (this.requestRepliesObservable == null) {
                this.requestRepliesSubject = new Subject<IEvent>()
                this.requestRepliesObservable = this._observe(this.replyToCri as string)
                                                    .pipe(multicast(this.requestRepliesSubject)) as ConnectableObservable<IEvent>
                this.requestRepliesSubscription = this.requestRepliesObservable.connect()
            }

            // Set once the server has finished the request, so there is nothing left to cancel. A request
            // the connection failed is not finished: with a sticky session the server is still running
            // it, so the cancel is still owed and goes out on the connection that replaces the lost one.
            let finished = false
            const correlationId = uuidv4()
            const defaultMessagesSubscription: Unsubscribable
                      = this.requestRepliesObservable
                            .pipe(filter((value: IEvent): boolean => {
                                return value.headers.get(EventConstants.CORRELATION_ID_HEADER) === correlationId
                            })).subscribe({
                                              next(value: IEvent): void {

                                                  if (value.hasHeader(EventConstants.CONTROL_HEADER)) {

                                                      if (value.headers.get(EventConstants.CONTROL_HEADER) === 'complete') {
                                                          finished = true
                                                          subscriber.complete()
                                                      } else {
                                                          // Thrown from here it would reach no one but the process
                                                          finished = true
                                                          subscriber.error(new Error('Control Header ' + value.headers.get(EventConstants.CONTROL_HEADER) + ' is not supported'))
                                                      }

                                                  } else if (value.hasHeader(EventConstants.ERROR_HEADER)) {

                                                      // TODO: add custom error type that contains error detail as well if provided by server, this would be the event body
                                                      finished = true
                                                      subscriber.error(new Error(value.getHeader(EventConstants.ERROR_HEADER)))

                                                  } else {

                                                      subscriber.next(value)

                                                  }
                                              },
                                              error(err: any): void {
                                                  subscriber.error(err)
                                              },
                                              complete(): void {
                                                  finished = true
                                                  subscriber.complete()
                                              }
                                          })

            subscriber.add(defaultMessagesSubscription)

            event.setHeader(EventConstants.REPLY_TO_HEADER, this.replyToCri as string)
            event.setHeader(EventConstants.CORRELATION_ID_HEADER, correlationId)

            this.send(event)

            return () => {
                if (sendControlEvents && !finished && this.stompConnectionManager.active) {
                    // create control event to cancel long-running request
                    const controlEvent: Event = new Event(event.cri)
                    controlEvent.setHeader(EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_CANCEL)
                    controlEvent.setHeader(EventConstants.CORRELATION_ID_HEADER, correlationId)
                    // A gateway with nothing to cancel - the service is gone - answers on the reply-to.
                    // Without one it has no way to say so except an ERROR that ends the connection.
                    controlEvent.setHeader(EventConstants.REPLY_TO_HEADER, this.replyToCri as string)
                    this.send(controlEvent)
                }
            }
        })
    }

    public listen(_serverInfo: ServerInfo): Promise<void> {
        return Promise.reject('Not implemented')
    }

    public observe(cri: string): Observable<IEvent> {
        return this._observe(cri)
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

    /**
     * Creates the proper error to return when the connection is not active on a send request
     */
    private createSendUnavailableError(): Error {
        if (this.closeError) {
            return new ContinuumError(`The event bus connection closed and must be connected again: ${this.closeError.message}`)
        }
        return new ContinuumError('You must call connect on the event bus before sending any request')
    }

    /**
     * This is internal impl of observe that creates a cold observable.
     * The public variants transform this to some type of hot observable depending on the need
     *
     * The subscription follows the connection: it is made on the connection that is active now and
     * again on each one that replaces it after a close, so a registered service is served on whatever
     * connection the caller establishes next. Within one connection rx-stomp re-subscribes on reconnect
     * by itself; across connections that is done here.
     * @param cri to observe
     * @return the cold {@link Observable<IEvent>} for the given destination
     */
    private _observe(cri: string): Observable<IEvent> {
        // Shared, so that one result subscribed many times is one subscription on the wire, as
        // rx-stomp's own watch() is
        return new Observable<IEvent>((subscriber) => {
            let current: Subscription | null = null
            const attach = (): void => {
                // Forwarded through a plain observer: handed the subscriber itself, RxJS would use it as
                // the inner subscription, and detaching from a closed connection would end the caller's too
                current = this.stompConnectionManager
                              .watch(cri)
                              .pipe(map<IMessage, IEvent>(EventBus.toEvent))
                              .subscribe({
                                             next: (event: IEvent) => subscriber.next(event),
                                             error: (error: any) => subscriber.error(error),
                                             complete: () => subscriber.complete()
                                         })
            }
            // Attached to the connection there is, if there is one; otherwise to the next one. Once
            // attached, it stays attached until that connection closes: rx-stomp re-subscribes within a
            // connection by itself, and a subscription made while connect() was pending was made by it
            // as the client came up, so connect() resolving is not a reason to make it again.
            if (this.stompConnectionManager.active) {
                attach()
            }
            const following = new Subscription()
            following.add(this.stompConnectionManager.events.subscribe(event => {
                if (event.type === 'closed') {
                    current?.unsubscribe()
                    current = null
                }
            }))
            following.add(this.activated.subscribe(() => {
                if (current == null) {
                    attach()
                }
            }))
            return () => {
                following.unsubscribe()
                current?.unsubscribe()
            }
        }).pipe(share())
    }

    /** We translate all IMessage objects to IEvent objects */
    private static toEvent(message: IMessage): IEvent {
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
    }

}
