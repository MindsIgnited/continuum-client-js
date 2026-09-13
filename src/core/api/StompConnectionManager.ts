import {ConnectionInfo} from '@/api/ConnectionInfo'
import {ConnectionRefusedError} from '@/api/errors/ConnectionRefusedError'
import {ContinuumError} from '@/api/errors/ContinuumError'
import {ConnectedInfo} from '@/api/security/ConnectedInfo'
import {EventConstants} from '@/core/api/IEventBus'
import {IFrame, IMessage, IRxStompPublishParams, RxStomp, RxStompConfig, RxStompState, StompHeaders} from '@stomp/rx-stomp'
import {ReconnectionTimeMode} from '@stomp/stompjs'
import {Observable, Subject} from 'rxjs'
import {v4 as uuidv4} from 'uuid'
import debug from 'debug'

/**
 * What the connection reports about itself, in the order it happens.
 *
 * `lost`: the socket of an established connection closed. Reconnection carries on underneath, but
 * anything in flight on the old socket cannot complete and should be failed now.
 *
 * `closed`: the connection is down and will not reconnect. Emitted exactly once per activation, after
 * the manager is already inactive, so a subscriber may call activate() again from inside the handler.
 * `error` says why when it was not asked for: the server refused us with an ERROR frame, or the bound
 * on reconnection attempts was reached. It is absent when deactivate() was called.
 */
export type ConnectionEvent =
    | { type: 'lost' }
    | { type: 'closed', error?: ContinuumError }

/**
 * Creates a new RxStomp client and manages it
 * This is here to simplify the logic needed for connection management and the usage of the client.
 *
 * The manager is a small state machine: inactive -> active -> closing -> inactive. Everything else about
 * the connection is reported through {@link events}; nothing here needs to be read back after the fact.
 */
export class StompConnectionManager {

    public readonly events: Observable<ConnectionEvent>
    private readonly eventSubject = new Subject<ConnectionEvent>()
    private readonly INITIAL_RECONNECT_DELAY: number = 2000
    private readonly MAX_RECONNECT_DELAY: number = 120000 // 2 mins
    private readonly JITTER_MAX: number = 5000
    /** How long a single attempt may take to reach CONNECTED before it is abandoned, unless the caller says otherwise */
    private readonly DEFAULT_CONNECT_TIMEOUT_MS: number = 10000
    /** How long a polite close waits for the peer's part in it before the socket is simply discarded */
    private readonly POLITE_CLOSE_GRACE_MS: number = 5000
    private debugLogger = debug('continuum:stomp')

    private state: 'inactive' | 'active' | 'closing' = 'inactive'
    private rxStomp: RxStomp | null = null
    /** The teardown in progress, so a deactivate() that overlaps another waits on it rather than repeating it */
    private closing: Promise<void> | null = null
    /** Why the teardown in progress is happening, if it has a reason; an overlapping close may supply one */
    private closingError: ContinuumError | undefined = undefined
    /** Whether the teardown in progress was asked for by the caller, in which case nothing that happens during it is a reason */
    private closeRequested: boolean = false
    /** Rejects the activate() promise, until the first CONNECTED frame settles it */
    private rejectActivation: ((error: ContinuumError) => void) | null = null

    private replyToId = uuidv4()
    private _replyToCri = EventConstants.SERVICE_DESTINATION_PREFIX + this.replyToId + ':' + uuidv4() + '@continuum.js.EventBus/replyHandler'

    // Per activation
    /** The session the server gave us, when sticky sessions are on. The only thing a reconnect presents. */
    private sessionId: string | null = null
    private connectionAttempts: number = 0
    private everConnected: boolean = false
    private lastWebsocketError: Event | null = null

    constructor() {
        this.events = this.eventSubject.asObservable()
    }

    /**
     * @return true if this {@link StompConnectionManager} is actively trying to maintain a connection to the Stomp server, false if not.
     */
    public get active(): boolean {
        return this.state === 'active'
    }

    public get replyToCri(): string {
        return this._replyToCri
    }

    /**
     * return true if this {@link StompConnectionManager} is active and has a connection to the stomp server
     */
    public get connected(): boolean {
        return this.active && this.rxStomp!.connected()
    }

    /**
     * Connects, and keeps reconnecting until {@link deactivate} is called, the server refuses a connect, or
     * {@link ConnectionInfo#maxConnectionAttempts} is reached. The returned promise settles on the first
     * CONNECTED frame; a refusal or exhausted bound before that rejects it, after that it is reported on
     * {@link events} as a `closed` event carrying the error.
     */
    public activate(connectionInfo: ConnectionInfo): Promise<ConnectedInfo> {
        if (!connectionInfo) {
            return Promise.reject(new ContinuumError('You must supply a valid connectionInfo object'))
        }
        if (!connectionInfo.host) {
            return Promise.reject(new ContinuumError('No host provided'))
        }
        if (this.state === 'closing') {
            return Promise.reject(new ContinuumError('Stomp connection is still closing'))
        }
        if (this.state === 'active') {
            return Promise.reject(new ContinuumError('Stomp connection already active'))
        }

        this.state = 'active'
        this.sessionId = null
        this.connectionAttempts = 0
        this.everConnected = false
        this.lastWebsocketError = null

        const url = 'ws' + (connectionInfo.useSSL ? 's' : '')
            + '://' + connectionInfo.host
            + (connectionInfo.port ? ':' + connectionInfo.port : '') + '/v1'
        // The WebSocket constructor throws on a bad URL, from inside a stompjs call nothing awaits, and
        // the attempt would neither fail nor proceed. Refused here instead, before anything is started.
        try {
            new URL(url)
        } catch (e) {
            this.state = 'inactive'
            return Promise.reject(new ContinuumError(`Invalid connection URL ${url}: ${(e as Error).message}`))
        }

        const rxStomp = new RxStomp()
        this.rxStomp = rxStomp

        return new Promise((resolve, reject): void => {
            this.rejectActivation = reject

            // This runs inside stompjs on the activation that scheduled it; if the manager has moved on
            // to another activation while it was waiting, whatever it was about to do is no longer wanted
            const abandoned = (): boolean => this.rxStomp !== rxStomp

            const stompConfig: RxStompConfig = {
                brokerURL: url,
                heartbeatIncoming: 120000,
                heartbeatOutgoing: 30000,
                reconnectDelay: this.INITIAL_RECONNECT_DELAY,
                // A peer that accepts the socket and then says nothing would otherwise hold the attempt
                // open forever; past this stompjs abandons it and it counts as a failed attempt
                connectionTimeout: connectionInfo.connectTimeoutMs ?? this.DEFAULT_CONNECT_TIMEOUT_MS,
                beforeConnect: async (): Promise<void> => {
                    if (abandoned()) {
                        return
                    }
                    // If max connections are set then make sure we have not exceeded that threshold
                    if (connectionInfo.maxConnectionAttempts) {
                        this.connectionAttempts++
                        if (this.connectionAttempts > connectionInfo.maxConnectionAttempts) {
                            const message = (this.lastWebsocketError as any)?.message ?? 'UNKNOWN'
                            await this.close(new ContinuumError(`Max number of reconnection attempts reached. Last WS Error ${message}`))
                            return
                        }
                    }
                    await this.connectionJitterDelay()
                    if (abandoned()) {
                        return
                    }
                    // Headers are built fresh for every attempt, so nothing is ever mutated: not the
                    // caller's object, and not what an earlier attempt sent. A caller's function that
                    // cannot answer ends the connection rather than the attempt: stompjs does not catch
                    // what escapes from here, and a rejection would leave everything hanging, still active.
                    let headers: StompHeaders
                    try {
                        headers = await this.connectHeadersForAttempt(connectionInfo)
                    } catch (e: any) {
                        if (!abandoned()) {
                            await this.close(new ContinuumError(`connectHeaders could not be produced: ${e?.message ?? e}`))
                        }
                        return
                    }
                    if (abandoned()) {
                        return
                    }
                    // use replyToId if provided in connectionInfo, otherwise set it
                    if (headers[EventConstants.REPLY_TO_ID_HEADER]) {
                        this.replyToId = headers[EventConstants.REPLY_TO_ID_HEADER]
                        this._replyToCri = EventConstants.SERVICE_DESTINATION_PREFIX + this.replyToId + ':' + uuidv4() + '@continuum.js.EventBus/replyHandler'
                    } else {
                        headers[EventConstants.REPLY_TO_ID_HEADER] = this.replyToId
                    }
                    rxStomp.stompClient.connectHeaders = headers
                }
            }

            if (this.debugLogger.enabled) {
                stompConfig.debug = (msg: string): void => {
                    this.debugLogger(msg)
                }
            }

            rxStomp.configure(stompConfig)

            // Set values that are only accessible from the stompClient
            rxStomp.stompClient.maxReconnectDelay = this.MAX_RECONNECT_DELAY
            rxStomp.stompClient.reconnectTimeMode = ReconnectionTimeMode.EXPONENTIAL

            rxStomp.webSocketErrors$.subscribe((value: Event) => {
                this.lastWebsocketError = value
            })

            // A STOMP ERROR frame means the server is done with us: it refused the connect, refused a
            // reconnect presenting a session it no longer has, or rejected something we sent. The library
            // cannot recover on its own - it holds no credentials, only the session id - so it closes and
            // leaves connecting again to the caller. The socket is discarded rather than closed politely;
            // there is nothing left to say, and waiting on the server's close would only delay the report.
            rxStomp.stompErrors$.subscribe((frame: IFrame) => {
                this.closeAfterCallback(new ConnectionRefusedError(frame), true)
            })

            // The socket of an established connection closing means whatever was in flight is gone.
            // Only reported when the close was not ours: a deactivate() ends in a `closed` event instead.
            let wasOpen = false
            rxStomp.connectionState$.subscribe((state: RxStompState) => {
                if (state === RxStompState.OPEN) {
                    wasOpen = true
                } else if (wasOpen && (state === RxStompState.CLOSING || state === RxStompState.CLOSED)) {
                    wasOpen = false
                    if (this.state === 'active') {
                        this.eventSubject.next({type: 'lost'})
                    }
                }
            })

            // This is triggered when the server sends a CONNECTED frame.
            rxStomp.serverHeaders$.subscribe((headers: StompHeaders) => {
                // The credentials that opened this connection have done their work. They are cleared in
                // place because stompjs keeps its own reference to this object; the next attempt is built afresh.
                const sent = rxStomp.stompClient.connectHeaders
                for (const key of Object.keys(sent)) {
                    delete sent[key]
                }

                const connectedInfo = this.parseConnectedInfo(headers, connectionInfo)
                if (connectedInfo == null) {
                    // The server has accepted us and holds a session, so it is told rather than cut off,
                    // which means the socket is not discarded from inside the callback that delivered CONNECTED
                    this.closeAfterCallback(new ContinuumError('Server did not return proper data for successful login'), false)
                    return
                }

                if (!connectionInfo.disableStickySession) {
                    this.sessionId = connectedInfo.sessionId
                }
                // Each reconnect gets the full bound of attempts; a connection that recovers has not spent any
                this.connectionAttempts = 0
                this.lastWebsocketError = null

                if (!this.everConnected) {
                    this.everConnected = true
                    this.rejectActivation = null
                    resolve(connectedInfo)
                }
            })

            rxStomp.activate()
        })
    }

    /**
     * Closes the connection and stops reconnecting. Safe to call at any time: it does nothing when
     * inactive, and a call that overlaps a close already in progress waits for that one.
     * @param force if true the socket is discarded rather than closed with a DISCONNECT frame
     */
    public deactivate(force?: boolean): Promise<void> {
        // From here on, the reason the connection ends is that it was asked to; whatever the server
        // says on the way out - an ERROR for something it was still processing, say - is not one
        if (this.state === 'active') {
            this.closeRequested = true
        }
        return this.close(undefined, force)
    }

    public publish(parameters: IRxStompPublishParams): void {
        this.requireActive().publish(parameters)
    }

    public watch(cri: string): Observable<IMessage> {
        return this.requireActive().watch(cri)
    }

    /**
     * The one path out of the active state. The `closed` event is emitted only once the manager is
     * inactive again, so a handler can activate() right away; the activate() promise, if still pending,
     * is rejected with the same error after that.
     */
    private close(error?: ContinuumError, force?: boolean): Promise<void> {
        if (this.state === 'inactive') {
            return Promise.resolve()
        }
        const rxStomp = this.rxStomp!
        if (this.closing) {
            // A close that overlaps one in progress adds what it knows. A reason, if the first had none
            // and was not the caller's own request: an ERROR frame arriving during a close the server
            // initiated is still the reason it ended. And force, which stompjs honours while already
            // deactivating - a polite close waiting on a peer that will never answer is exactly what a
            // forced one is for.
            if (!this.closeRequested) {
                this.closingError ??= error
            }
            if (force) {
                rxStomp.deactivate({force: true})
                       .catch(e => this.debugLogger(`Error forcing a close already in progress: ${e}`))
            }
            return this.closing
        }
        this.state = 'closing'
        this.closingError = this.closeRequested ? undefined : error
        this.closing = (async (): Promise<void> => {
            try {
                await this.deactivateWithinGrace(rxStomp, force)
            } finally {
                const rejectActivation = this.rejectActivation
                const closedWith = this.closingError
                this.rejectActivation = null
                this.closingError = undefined
                this.closeRequested = false
                this.rxStomp = null
                this.closing = null
                this.state = 'inactive'
                this.eventSubject.next({type: 'closed', error: closedWith})
                if (rejectActivation) {
                    rejectActivation(closedWith ?? new ContinuumError('Connection was closed before it was established'))
                }
            }
        })()
        return this.closing
    }

    /**
     * A polite close - DISCONNECT, then wait for the RECEIPT and the socket's own close - is owed to a
     * server holding a session, but a peer that never does its part does not get to hold the caller
     * on it. Past the grace period the socket is discarded and the same promise settles.
     */
    private async deactivateWithinGrace(rxStomp: RxStomp, force?: boolean): Promise<void> {
        const closing = rxStomp.deactivate({force: force})
        if (force) {
            return closing
        }
        let graceTimer: ReturnType<typeof setTimeout> | undefined
        const graceExpired = new Promise<boolean>(resolve => {
            graceTimer = setTimeout(() => resolve(true), this.POLITE_CLOSE_GRACE_MS)
        })
        const timedOut = await Promise.race([closing.then(() => false, () => false), graceExpired])
        clearTimeout(graceTimer)
        if (timedOut) {
            this.debugLogger(`Polite close not completed within ${this.POLITE_CLOSE_GRACE_MS}ms, discarding the socket`)
            rxStomp.deactivate({force: true})
                   .catch(e => this.debugLogger(`Error discarding the socket after the grace period: ${e}`))
        }
        return closing
    }

    /**
     * A close asked for from inside one of rx-stomp's own callbacks. It is deferred a tick so the
     * callback's caller finishes first: a browser WebSocket has no terminate(), and the one stompjs
     * installs runs the close handlers synchronously, which from inside onConnect would tear the
     * client down before rx-stomp had finished bringing it up.
     */
    private closeAfterCallback(error: ContinuumError, force: boolean): void {
        queueMicrotask(() => {
            this.close(error, force).catch(e => this.debugLogger(`Error closing after ${error.message}: ${e}`))
        })
    }

    /** The connected info the server sent, or null if it sent none or something unreadable */
    private parseConnectedInfo(headers: StompHeaders, connectionInfo: ConnectionInfo): ConnectedInfo | null {
        const connectedInfoJson: string | undefined = headers[EventConstants.CONNECTED_INFO_HEADER]
        if (connectedInfoJson == null) {
            return null
        }
        let connectedInfo: ConnectedInfo
        try {
            // STOMP 1.2 says CONNECTED is not escaped, and stompjs takes it at its word. The gateway's
            // codec (vertx-stomp-lite HeaderCodec.encode) nonetheless doubles every backslash in the
            // value, which leaves any JSON escape in it - a quote in a participant's name, say - unreadable
            // until that is undone.
            connectedInfo = JSON.parse(connectedInfoJson.replace(/\\\\/g, '\\'))
        } catch (e) {
            this.debugLogger(`Unreadable ${EventConstants.CONNECTED_INFO_HEADER} header: ${e}`)
            return null
        }
        if (connectedInfo == null || typeof connectedInfo !== 'object') {
            return null
        }
        if (!connectionInfo.disableStickySession && (connectedInfo.sessionId == null || connectedInfo.replyToId == null)) {
            return null
        }
        return connectedInfo
    }

    /**
     * The CONNECT headers for one attempt. With sticky sessions a reconnect presents only the session
     * the server gave us; the library never holds the caller's credentials past the attempt that used
     * them. Otherwise the caller's headers are sent - copied if static, called again if a function,
     * which is what a function is for: credentials that may have changed since the last attempt.
     */
    private async connectHeadersForAttempt(connectionInfo: ConnectionInfo): Promise<StompHeaders> {
        if (this.sessionId != null) {
            return {[EventConstants.SESSION_HEADER]: this.sessionId}
        }

        const supplied = typeof connectionInfo.connectHeaders === 'function'
            ? await connectionInfo.connectHeaders()
            : connectionInfo.connectHeaders
        const headers: StompHeaders = {...supplied}

        if (connectionInfo.disableStickySession) {
            headers[EventConstants.DISABLE_STICKY_SESSION_HEADER] = 'true'
        }
        return headers
    }

    private requireActive(): RxStomp {
        if (!this.active) {
            throw new ContinuumError('You must call connect on the event bus before sending any request')
        }
        return this.rxStomp!
    }

    /**
     * Make sure clients don't all try to reconnect at the same time.
     */
    private async connectionJitterDelay(): Promise<void> {
        if (this.everConnected) {
            const randomJitter = Math.random() * this.JITTER_MAX;
            this.debugLogger(`Adding ${randomJitter}ms of jitter delay`)
            return new Promise(resolve => setTimeout(resolve, randomJitter));
        }
    }

}
