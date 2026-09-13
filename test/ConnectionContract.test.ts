import {afterEach, beforeEach, describe, expect, it} from 'vitest'
import {createServer, Server, Socket} from 'node:net'
import {WebSocket} from 'ws'
import {IFrame} from '@stomp/rx-stomp'
import {
    ConnectedInfo,
    ConnectionInfo,
    ConnectionLostError,
    ConnectionRefusedError,
    ContinuumError,
    ContinuumSingleton
} from '../src'
import {ServiceIdentifier} from '../src/core/api/ServiceIdentifier'
import {GatewayConnection, ScriptedGateway} from './ScriptedGateway'

// This is required when running Continuum from node
Object.assign(global, {WebSocket})

/**
 * The connection contract, one case per way a client was found able to hang, leak or lie, each driven
 * against a scripted gateway so the situation is produced on demand rather than hoped for. A case here
 * is written before its fix: it fails on the behaviour it describes, and passing is what "fixed" means.
 */
describe('Connection contract', () => {
    let gateway: ScriptedGateway
    let continuum: ContinuumSingleton

    beforeEach(async () => {
        gateway = await ScriptedGateway.start()
        continuum = new ContinuumSingleton()
    })

    afterEach(async () => {
        await Promise.race([continuum.disconnect(true), sleep(5000)]).catch(() => {})
        await gateway.stop()
    })

    function connectionInfo(overrides: Partial<ConnectionInfo> = {}): ConnectionInfo {
        return Object.assign(new ConnectionInfo(), {
            host: gateway.host,
            port: gateway.port,
            connectHeaders: {login: 'guest', passcode: 'guest'},
            ...overrides
        })
    }

    it('disconnect(true) breaks a graceful close the server is not answering', {timeout: 30000}, async () => {
        // A half-open peer: DISCONNECT goes out, the RECEIPT never comes back, and without heartbeats the
        // graceful close waits on it indefinitely. A forced close is the caller's only way out.
        gateway.onDisconnect = 'ignore'
        await continuum.connect(connectionInfo())
        const connection = await gateway.waitForConnection(0)

        const graceful = continuum.disconnect()
        await connection.waitForFrame(f => f.command === 'DISCONNECT', 5000, 'the DISCONNECT frame')
        expect(await outcome(graceful, 2000), 'the graceful close is waiting on a RECEIPT that will not come').toBe('pending')

        const forced = continuum.disconnect(true)
        expect(await outcome(forced, 5000), 'a forced close must not be folded into the graceful one it is meant to break').toBe('settled')
        expect(await outcome(graceful, 1000), 'and the graceful close settles with it').toBe('settled')
        expect(continuum.eventBus.isConnectionActive()).toBe(false)
    })

    it('a stream interrupted by a socket loss still cancels its invocation once reconnected', {timeout: 60000}, async () => {
        // With a sticky session the server keeps the invocation running across the socket loss and keeps
        // producing into the reply address. The stream is failed on the client, so its cancel must still
        // reach the server, on the connection that replaces the lost one - and must be a cancel the
        // gateway can answer: one without a reply-to is answered with an ERROR that ends the connection.
        gateway.handlers.set('srv://com.example.Svc/watch', () => { /* a long-running stream: never answers */ })
        const fatal: ContinuumError[] = []
        continuum.eventBus.fatalErrors.subscribe(e => fatal.push(e))
        await continuum.connect(connectionInfo())
        const first = await gateway.waitForConnection(0)

        let streamError: unknown = null
        continuum.serviceProxy('com.example.Svc').invokeStream('watch', []).subscribe({error: e => streamError = e})
        const request = await first.waitForFrame(f => f.command === 'SEND' && f.headers['destination'] === 'srv://com.example.Svc/watch',
                                                 5000, 'the stream request')
        const correlationId = request.headers['__correlation-id']
        expect(correlationId).toBeDefined()

        first.drop()
        const second = await gateway.waitForConnection(1)
        await second.waitForFrame(f => f.command === 'CONNECT', 10000, 'the reconnect')
        expect(second.frames[0].headers['session'], 'the reconnect presents the session the invocation lives in').toBe(first.sessionId)

        const cancel = await second.waitForFrame(f => f.command === 'SEND'
                                                      && f.headers['control'] === 'cancel'
                                                      && f.headers['__correlation-id'] === correlationId,
                                                 10000, 'the cancel for the failed stream')
        expect(streamError).toBeInstanceOf(ConnectionLostError)
        expect(cancel.headers['reply-to'], 'a cancel carries a reply-to, so a gateway with nothing to cancel can answer it politely').toBeDefined()
        await sleep(500)
        expect(fatal, 'the recovered connection survives its own cancel').toEqual([])
        expect(continuum.eventBus.isConnected()).toBe(true)
    })

    it('a CONNECTED frame without connected-info is refused politely, even where terminate() is synchronous', {timeout: 30000}, async () => {
        // The server accepted and registered a session before the client found the frame unusable, so the
        // client owes it a DISCONNECT rather than a discarded socket. And a browser WebSocket has no
        // terminate(): stompjs installs one that runs the close handlers synchronously, inside whichever
        // rx-stomp callback asked for it. Node's ws has its own, which is why the suite never saw that.
        class BrowserLikeWebSocket extends WebSocket {
            constructor(url: string, protocols?: string | string[]) {
                super(url, protocols);
                (this as any).terminate = undefined
            }
        }
        Object.assign(global, {WebSocket: BrowserLikeWebSocket})
        const uncaught: unknown[] = []
        const collect = (e: unknown): void => { uncaught.push(e) }
        process.on('uncaughtException', collect)
        process.on('unhandledRejection', collect)
        try {
            gateway.onConnect = () => ({connectedHeaders: {version: '1.2', 'heart-beat': '0,0'}})

            // Something has to be watching when CONNECTED arrives: rx-stomp subscribes it as it brings the
            // client up, and a teardown that runs synchronously from inside that would pull the client out
            // from under it
            const connecting = continuum.connect(connectionInfo())
            continuum.serviceRegistry.register(new ServiceIdentifier('com.example', 'Watcher'), {noop: () => undefined})
            await expect(settles(connecting, 10000)).rejects.toThrow(/proper data/)
            expect(continuum.eventBus.isConnectionActive()).toBe(false)
            const connection = await gateway.waitForConnection(0)
            await connection.waitForFrame(f => f.command === 'DISCONNECT', 5000, 'a DISCONNECT for the session it accepted')
            await sleep(500)
            expect(uncaught, 'nothing escapes to the process').toEqual([])
        } finally {
            process.off('uncaughtException', collect)
            process.off('unhandledRejection', collect)
            Object.assign(global, {WebSocket})
        }
    })

    it('credentials are not kept once a sticky session is established', {timeout: 30000}, async () => {
        // The library's claim is that after the first connect it holds the session id and nothing else.
        // That is a claim about what it holds, so this looks at what it holds.
        await continuum.connect(connectionInfo())
        const manager = (continuum.eventBus as any).stompConnectionManager
        const client = manager.rxStomp.stompClient
        // stompjs's handler keeps its own reference to the headers it sent, so both are checked
        for (const held of [client.connectHeaders, client._stompHandler.connectHeaders] as Record<string, string>[]) {
            expect(Object.keys(held), 'the credentials that opened the session must not outlive the attempt that used them')
                .not.toEqual(expect.arrayContaining(['login', 'passcode']))
        }
    })

    it('maxConnectionAttempts bounds each reconnect, not the life of the connection', {timeout: 60000}, async () => {
        // Two attempts is enough for every individual reconnect here; it would only be exhausted if the
        // attempts were counted across the whole connection, which would call the third reconnect fatal.
        const fatal = new Promise<never>((_, reject) =>
            continuum.eventBus.fatalErrors.subscribe(e => reject(new Error(`reported as fatal: ${e.message}`))))
        await continuum.connect(connectionInfo({maxConnectionAttempts: 2}))

        for (let drop = 0; drop < 2; drop++) {
            const current = await gateway.waitForConnection(drop)
            current.drop()
            // A connection that recovers from each loss on the first try has nothing fatal to report
            const next = await Promise.race([gateway.waitForConnection(drop + 1), fatal])
            await next.waitForFrame(f => f.command === 'CONNECT', 15000, `reconnect ${drop + 1}`)
            await waitFor(() => continuum.eventBus.isConnected(), 5000)
            expect(continuum.eventBus.isConnected(), `connected again after socket loss ${drop + 1}`).toBe(true)
        }
    })

    it('a connectHeaders function that fails does not leave connect() hanging', {timeout: 30000}, async () => {
        const info = connectionInfo({connectHeaders: async () => { throw new Error('token refresh failed') }})
        await expect(settles(continuum.connect(info), 10000)).rejects.toThrow(/token refresh failed/)
        expect(continuum.eventBus.isConnectionActive(), 'and does not leave the connection active').toBe(false)
    })

    it('a connectHeaders function that fails on reconnect closes the connection and says so', {timeout: 60000}, async () => {
        // Without sticky sessions every reconnect asks the function again; when it cannot answer the
        // connection cannot continue, and that has to be reported rather than retried into silence.
        let calls = 0
        const info = connectionInfo({
            disableStickySession: true,
            connectHeaders: async () => {
                if (++calls > 1) {
                    throw new Error('token refresh failed')
                }
                return {login: 'guest', passcode: 'guest'}
            }
        })
        const fatal = new Promise<ContinuumError>(resolve => continuum.eventBus.fatalErrors.subscribe(resolve))
        await continuum.connect(info)
        const first = await gateway.waitForConnection(0)

        first.drop()
        const reported = await settles(fatal, 20000)
        expect(reported.message).toMatch(/token refresh failed/)
        expect(continuum.eventBus.isConnectionActive(), 'reported once the connection is already down').toBe(false)
    })

    it('a malformed connected-info header rejects connect() rather than hanging it', {timeout: 30000}, async () => {
        gateway.onConnect = () => ({connectedHeaders: {version: '1.2', 'heart-beat': '0,0', 'connected-info': '{not json'}})
        await expect(settles(continuum.connect(connectionInfo()), 10000)).rejects.toThrow(/proper data/)
        expect(continuum.eventBus.isConnectionActive()).toBe(false)
        const connection = await gateway.waitForConnection(0)
        await connection.waitForFrame(f => f.command === 'DISCONNECT', 5000, 'a DISCONNECT for the session it accepted')
    })

    it('errors carry their names', () => {
        const frame = {command: 'ERROR', headers: {message: 'nope'}, body: 'nope', isBinaryBody: false, binaryBody: new Uint8Array()} as IFrame
        const errors: [Error, string][] = [
            [new ContinuumError('nope'), 'ContinuumError'],
            [new ConnectionLostError('nope'), 'ConnectionLostError'],
            [new ConnectionRefusedError(frame), 'ConnectionRefusedError'],
        ]
        for (const [error, name] of errors) {
            expect(error.name).toBe(name)
            expect(String(error), 'a caller that stringifies the rejection sees what it was').toBe(`${name}: nope`)
            expect(error).toBeInstanceOf(ContinuumError)
        }
    })

    it('a registered service is served again after the caller reconnects from fatalErrors', {timeout: 60000}, async () => {
        // fatalErrors -> connect() is the documented recovery. A service registered before the loss has to
        // come back with it, which shows on the wire as its subscription being made on the new connection.
        const info = connectionInfo()
        await continuum.connect(info)
        continuum.serviceRegistry.register(new ServiceIdentifier('com.example', 'Echo'), {echo: (v: string) => v})
        const first = await gateway.waitForConnection(0)
        const isServiceSubscription = (f: { command: string, headers: Record<string, string> }): boolean =>
            f.command === 'SUBSCRIBE' && (f.headers['destination'] ?? '').includes('com.example.Echo')
        await first.waitForFrame(isServiceSubscription, 5000, 'the service subscription')

        const recovered = new Promise<ConnectedInfo>((resolve, reject) =>
            continuum.eventBus.fatalErrors.subscribe(() => continuum.connect(info).then(resolve, reject)))

        // The instance is replaced by one with no memory of the session; the reconnect is refused
        gateway.sessions.clear()
        first.drop()
        await settles(recovered, 30000)
        const replacement: GatewayConnection = await gateway.waitForConnection(2)
        await replacement.waitForFrame(isServiceSubscription, 10000, 'the service subscription on the new connection')
    })

    it('a reconnect attempt abandoned by disconnect() cannot touch the connection that replaced it', {timeout: 60000}, async () => {
        // A reconnect is waiting on the caller's connectHeaders() when the caller disconnects and connects
        // again with different credentials. When the old attempt's function finally answers - here, by
        // failing - it belongs to a connection that no longer exists and must act on nothing.
        let calls = 0
        let failOld: (e: Error) => void = () => undefined
        const old = connectionInfo({
            disableStickySession: true,
            connectHeaders: () => {
                if (++calls === 1) {
                    return Promise.resolve({login: 'guest', passcode: 'guest'})
                }
                return new Promise((_, reject) => { failOld = reject })
            }
        })
        const fatal: ContinuumError[] = []
        continuum.eventBus.fatalErrors.subscribe(e => fatal.push(e))
        await continuum.connect(old)
        const first = await gateway.waitForConnection(0)

        first.drop()
        await waitFor(() => calls === 2, 15000)
        expect(calls, 'the reconnect is waiting on the old credentials').toBe(2)

        await continuum.disconnect()
        await continuum.connect(connectionInfo())
        const replacement = await gateway.waitForConnection(1)
        expect(continuum.eventBus.isConnected()).toBe(true)

        failOld(new Error('token refresh failed (old)'))
        await sleep(1000)
        expect(fatal, 'nothing about the abandoned attempt is reported').toEqual([])
        expect(continuum.eventBus.isConnected(), 'and the replacement is untouched').toBe(true)
        expect(replacement.frames.filter(f => f.command === 'DISCONNECT'), 'no DISCONNECT went out on it').toEqual([])
    })

    it('an ERROR frame that arrives during a requested disconnect is not reported as fatal', {timeout: 30000}, async () => {
        // The gateway can answer a DISCONNECT with an ERROR when something it was still processing for
        // the connection fails. The caller asked for the close; the reason it ended is that the caller asked.
        gateway.onDisconnect = 'error'
        const fatal: ContinuumError[] = []
        continuum.eventBus.fatalErrors.subscribe(e => fatal.push(e))
        await continuum.connect(connectionInfo())

        await settles(continuum.disconnect(), 10000)
        await sleep(500)
        expect(continuum.eventBus.isConnectionActive()).toBe(false)
        expect(fatal, 'a close the caller asked for is never fatal, whatever the server said on the way out').toEqual([])
    })

    it('connected-info survives the escaping the gateway applies to CONNECTED', {timeout: 30000}, async () => {
        // The gateway's codec doubles backslashes in CONNECTED header values, and STOMP says CONNECTED is
        // not escaped, so the client is handed JSON it has to undo that on before it is readable.
        gateway.onConnect = () => ({accept: true, participant: {id: 'DOMAIN\\user', metadata: {name: 'Robert "Bob" Smith'}}})
        const connectedInfo = await settles(continuum.connect(connectionInfo()), 10000)
        expect(connectedInfo.participant.id).toBe('DOMAIN\\user')
        expect((connectedInfo.participant.metadata as any).name).toBe('Robert "Bob" Smith')
    })

    it('an observe() made while connect() is pending is subscribed once', {timeout: 30000}, async () => {
        // A subscription made before CONNECTED is made by rx-stomp as the client comes up. It must not be
        // made a second time when connect() then resolves: the first is torn down and anything already
        // dispatched to it is lost.
        const connecting = continuum.connect(connectionInfo())
        continuum.serviceRegistry.register(new ServiceIdentifier('com.example', 'Early'), {noop: () => undefined})
        await connecting
        await sleep(500)
        const first = await gateway.waitForConnection(0)
        const ofService = (command: string): number =>
            first.frames.filter(f => f.command === command && (f.headers['destination'] ?? '').includes('com.example.Early')).length
        expect(first.frames.filter(f => f.command === 'UNSUBSCRIBE'), 'nothing was unsubscribed').toEqual([])
        expect(ofService('SUBSCRIBE'), 'subscribed exactly once').toBe(1)
    })

    it('connect() gives up on a handshake that never completes', {timeout: 30000}, async () => {
        // A peer that accepts the socket and then says nothing: no CONNECTED, no error, no close.
        const accepted: Socket[] = []
        const silent: Server = createServer(socket => { accepted.push(socket) /* and never answer */ })
        await new Promise<void>(resolve => silent.listen(0, '127.0.0.1', resolve))
        try {
            const port = (silent.address() as { port: number }).port
            const info = connectionInfo({port, maxConnectionAttempts: 1, connectTimeoutMs: 1000})
            await expect(settles(continuum.connect(info), 10000)).rejects.toThrow(/Max number of reconnection attempts/)
            expect(continuum.eventBus.isConnectionActive()).toBe(false)
        } finally {
            accepted.forEach(socket => socket.destroy())
            await new Promise<void>(resolve => silent.close(() => resolve()))
        }
    })

    it('connected-info that is JSON but not an object is refused, not thrown', {timeout: 30000}, async () => {
        const uncaught: unknown[] = []
        const collect = (e: unknown): void => { uncaught.push(e) }
        process.on('uncaughtException', collect)
        process.on('unhandledRejection', collect)
        try {
            gateway.onConnect = () => ({connectedHeaders: {version: '1.2', 'heart-beat': '0,0', 'connected-info': 'null'}})
            await expect(settles(continuum.connect(connectionInfo()), 10000)).rejects.toThrow(/proper data/)
            expect(continuum.eventBus.isConnectionActive()).toBe(false)
            await sleep(500)
            expect(uncaught, 'nothing escapes to the process').toEqual([])
        } finally {
            process.off('uncaughtException', collect)
            process.off('unhandledRejection', collect)
        }
    })

    it('a polite close the peer never acknowledges is bounded', {timeout: 30000}, async () => {
        // A CONNECTED frame the client cannot use is answered with a DISCONNECT, out of courtesy to a
        // server holding a session. Courtesy has a limit: a peer that never sends the RECEIPT does not
        // get to keep connect() pending.
        gateway.onDisconnect = 'ignore'
        gateway.onConnect = () => ({connectedHeaders: {version: '1.2', 'heart-beat': '0,0'}})
        await expect(settles(continuum.connect(connectionInfo()), 15000)).rejects.toThrow(/proper data/)
        expect(continuum.eventBus.isConnectionActive()).toBe(false)
    })

    it('a service registered before connect() is served once connected', {timeout: 30000}, async () => {
        // Services are registered at construction, by decorator; whether the connection is up yet is not
        // theirs to know. The subscription is made when there is a connection to make it on.
        continuum.serviceRegistry.register(new ServiceIdentifier('com.example', 'Eager'), {noop: () => undefined})
        await continuum.connect(connectionInfo())
        const first = await gateway.waitForConnection(0)
        await first.waitForFrame(f => f.command === 'SUBSCRIBE' && (f.headers['destination'] ?? '').includes('com.example.Eager'),
                                 5000, 'the service subscription')
    })

    it('an unusable host rejects connect() rather than hanging it', {timeout: 30000}, async () => {
        const info = connectionInfo({host: 'localhost:8080'})
        await expect(settles(continuum.connect(info), 5000)).rejects.toBeInstanceOf(ContinuumError)
        expect(continuum.eventBus.isConnectionActive()).toBe(false)
    })

    it('a stalled handshake is abandoned at connectTimeoutMs, not at the socket\'s own close timeout', {timeout: 30000}, async () => {
        // The socket is accepted and the CONNECT never answered. Abandoning the attempt has to discard the
        // socket: closing it politely waits on a peer that is not there, for as long as the socket
        // library cares to wait, and that time is charged to every attempt.
        gateway.onConnect = () => ({silent: true})
        const info = connectionInfo({connectTimeoutMs: 1000, maxConnectionAttempts: 1})
        await expect(settles(continuum.connect(info), 6000)).rejects.toThrow(/Max number of reconnection attempts/)
        expect(continuum.eventBus.isConnectionActive()).toBe(false)
    })

    it('connectTimeoutMs of zero does not switch the bound off', {timeout: 30000}, async () => {
        gateway.onConnect = () => ({silent: true})
        const info = connectionInfo({connectTimeoutMs: 0, maxConnectionAttempts: 1})
        await expect(settles(continuum.connect(info), 20000)).rejects.toThrow(/Max number of reconnection attempts/)
    })

    it('a service reply that lands during a requested disconnect does not escape the process', {timeout: 30000}, async () => {
        // A hosted service finishes a call after its host asked to disconnect. There is nowhere to send
        // the reply; that is a fact to log, not an exception to let loose from a handler nothing awaits.
        const uncaught: unknown[] = []
        const collect = (e: unknown): void => { uncaught.push(e) }
        process.on('uncaughtException', collect)
        process.on('unhandledRejection', collect)
        try {
            let finish: (v: string) => void = () => undefined
            let entered = false
            // The supervisor maps prototype methods, as a decorated service class has
            class SlowService {
                wait(): Promise<string> {
                    entered = true
                    return new Promise(resolve => { finish = resolve })
                }
            }
            continuum.serviceRegistry.register(new ServiceIdentifier('com.example', 'Slow'), new SlowService())
            await continuum.connect(connectionInfo())
            const first = await gateway.waitForConnection(0)
            const subscription = await first.waitForFrame(f => f.command === 'SUBSCRIBE' && (f.headers['destination'] ?? '').includes('com.example.Slow'),
                                                          5000, 'the service subscription')
            first.sendMessage(subscription.headers['destination'] + '/wait',
                              {'reply-to': 'srv://caller@continuum.js.EventBus/replyHandler', '__correlation-id': 'c1', 'content-type': 'application/json'},
                              '[]', subscription.headers['destination'])
            await waitFor(() => entered, 5000)
            expect(entered, 'the service is in the middle of the call').toBe(true)

            gateway.onDisconnect = 'ignore'
            const closing = continuum.disconnect()
            await sleep(200)
            finish('done')
            await sleep(500)
            expect(uncaught, 'nothing escapes to the process').toEqual([])
            await settles(closing, 10000)
        } finally {
            process.off('uncaughtException', collect)
            process.off('unhandledRejection', collect)
        }
    })

    it('a stream created before a close does not decide where the next connection\'s replies are watched', {timeout: 30000}, async () => {
        // Subscribing after the close to a stream created before it must fail, not quietly set up the
        // reply address of a connection that no longer exists - the next connection has its own, and a
        // request on it must get its reply.
        gateway.handlers.set('srv://com.example.Svc/echo', (frame, connection) =>
            connection.sendMessage(frame.headers['reply-to'],
                                   {'__correlation-id': frame.headers['__correlation-id'], 'content-type': 'application/json'},
                                   '"ok"'))
        const info = connectionInfo({connectHeaders: {login: 'guest', passcode: 'guest', 'reply-to-id': 'fixed-by-caller'}})
        await continuum.connect(info)
        const proxy = continuum.serviceProxy('com.example.Svc')
        const stale = proxy.invokeStream('echo', [])
        await continuum.disconnect()

        let staleError: unknown = null
        stale.subscribe({error: e => staleError = e})
        await waitFor(() => staleError != null, 2000)
        expect(staleError, 'a stream subscribed after the close fails at once').toBeInstanceOf(ContinuumError)

        await continuum.connect(info)
        expect(await settles(proxy.invoke('echo', []), 5000), 'a request on the new connection gets its reply').toBe('ok')
    })

    it('disconnect() during a close the server started is still a requested close', {timeout: 30000}, async () => {
        // The server sent an ERROR and the socket is on its way down when the app, shutting down, calls
        // disconnect(). The app asked to be disconnected; reporting the close as fatal would have its
        // recovery handler reconnect in the middle of its shutdown.
        const fatal: ContinuumError[] = []
        continuum.eventBus.fatalErrors.subscribe(e => fatal.push(e))
        await continuum.connect(connectionInfo())
        const first = await gateway.waitForConnection(0)

        // Placed after the manager's own handler and deferred past the tick it defers its close to, so
        // disconnect() lands while that close is already in progress
        const manager = (continuum.eventBus as any).stompConnectionManager
        let disconnected: Promise<void> | null = null
        manager.rxStomp.stompErrors$.subscribe(() => queueMicrotask(() => queueMicrotask(() => {
            expect(continuum.eventBus.isConnectionActive(), 'the server-started close is in progress').toBe(false)
            disconnected = continuum.disconnect()
        })))
        first.sendError('Something the server was doing for you failed')
        await waitFor(() => disconnected != null, 2000)
        await settles(disconnected!, 10000)
        await sleep(200)
        expect(fatal, 'a close the caller asked for is never fatal').toEqual([])
    })

    it('an unsupported control value fails the request, not the process', {timeout: 30000}, async () => {
        const uncaught: unknown[] = []
        const collect = (e: unknown): void => { uncaught.push(e) }
        process.on('uncaughtException', collect)
        process.on('unhandledRejection', collect)
        try {
            gateway.handlers.set('srv://com.example.Svc/ctl', (frame, connection) =>
                connection.sendMessage(frame.headers['reply-to'],
                                       {'__correlation-id': frame.headers['__correlation-id'], control: 'suspend'}))
            await continuum.connect(connectionInfo())
            await expect(settles(continuum.serviceProxy('com.example.Svc').invoke('ctl', []), 5000)).rejects.toThrow(/not supported/)
            await sleep(300)
            expect(uncaught, 'nothing escapes to the process').toEqual([])
        } finally {
            process.off('uncaughtException', collect)
            process.off('unhandledRejection', collect)
        }
    })

    it('a host the WebSocket constructor refuses rejects connect() rather than hanging it', {timeout: 30000}, async () => {
        // A URL that parses but a WebSocket will not open; the constructor throws inside a stompjs call
        // nothing awaits, and without help the attempt neither fails nor proceeds
        const info = connectionInfo({host: `${gateway.host}#prod`})
        await expect(settles(continuum.connect(info), 5000)).rejects.toBeInstanceOf(ContinuumError)
        expect(continuum.eventBus.isConnectionActive()).toBe(false)
    })

    it('connected-info that is a JSON array is refused', {timeout: 30000}, async () => {
        gateway.onConnect = () => ({connectedHeaders: {version: '1.2', 'heart-beat': '0,0', 'connected-info': '[]'}})
        await expect(settles(continuum.connect(connectionInfo({disableStickySession: true})), 10000)).rejects.toThrow(/proper data/)
    })

    it('one observe() result subscribed twice is one subscription on the wire', {timeout: 30000}, async () => {
        await continuum.connect(connectionInfo())
        const events = continuum.eventBus.observe('srv://com.example.Shared')
        const a = events.subscribe()
        const b = events.subscribe()
        await sleep(500)
        const first = await gateway.waitForConnection(0)
        expect(first.frames.filter(f => f.command === 'SUBSCRIBE' && f.headers['destination'] === 'srv://com.example.Shared').length).toBe(1)
        a.unsubscribe()
        b.unsubscribe()
    })
})

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

/** Whether the promise settles within the budget; what it settled to is not the question */
async function outcome(promise: Promise<unknown>, withinMs: number): Promise<'settled' | 'pending'> {
    return Promise.race([promise.then(() => 'settled' as const, () => 'settled' as const), sleep(withinMs).then(() => 'pending' as const)])
}

/** The promise's own outcome, or a failure naming the hang if it has none within the budget */
function settles<T>(promise: Promise<T>, withinMs: number): Promise<T> {
    return Promise.race([promise, sleep(withinMs).then(() => Promise.reject(new Error(`did not settle within ${withinMs}ms`)))])
}

async function waitFor(predicate: () => boolean, budgetMs: number, intervalMs = 200): Promise<void> {
    const deadline = Date.now() + budgetMs
    while (Date.now() < deadline && !predicate()) {
        await sleep(intervalMs)
    }
}
