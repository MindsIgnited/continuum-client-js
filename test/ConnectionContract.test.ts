import {afterEach, beforeEach, describe, expect, it} from 'vitest'
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
        gateway.ackDisconnect = false
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
        // reach the server, on the connection that replaces the lost one.
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

        await second.waitForFrame(f => f.command === 'SEND'
                                       && f.headers['control'] === 'cancel'
                                       && f.headers['__correlation-id'] === correlationId,
                                  10000, 'the cancel for the failed stream')
        expect(streamError).toBeInstanceOf(ConnectionLostError)
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

            await expect(settles(continuum.connect(connectionInfo()), 10000)).rejects.toThrow(/proper data/)
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
        const held: Record<string, string> = manager.rxStomp.stompClient.connectHeaders
        expect(Object.keys(held), 'the credentials that opened the session must not outlive the attempt that used them')
            .not.toEqual(expect.arrayContaining(['login', 'passcode']))
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
