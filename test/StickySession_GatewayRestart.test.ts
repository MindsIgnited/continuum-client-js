import {afterAll, beforeAll, describe, expect, it} from 'vitest'
import {WebSocket} from 'ws'
import {
    ConnectedInfo,
    ConnectionInfo,
    ConnectionLostError,
    ConnectionRefusedError,
    ContinuumError,
    ContinuumSingleton,
    IServiceProxy
} from '../src'
import {StartedTestContainer} from 'testcontainers'
import {GATEWAY_IMAGE, startGateway} from './GatewayContainer'
import {logFailure, validateConnectedInfo} from './TestHelper'

// This is required when running Continuum from node
Object.assign(global, { WebSocket})

/**
 * What a client is entitled to when the server it is attached to goes away and its sticky session
 * does not survive - the single instance case, where the session lived only in that instance.
 *
 * The library holds no credentials after the first connect, only the session id, so it cannot
 * recover on its own. What it must do instead, and what this pins:
 *
 *   1. a request in flight when the socket closes is failed then, with ConnectionLostError, rather
 *      than left waiting for a reply the lost server can no longer send
 *   2. when the reconnect is refused, the refusal is reported on fatalErrors as a ConnectionRefusedError
 *      carrying the server's message, and the connection is already down - not left silently half alive
 *   3. the caller is free to connect again with credentials of its choosing, right there in the
 *      fatalErrors handler, and it works
 *
 * Before this contract the refused reconnect deactivated the transport without running cleanup, so
 * pending requests waited forever, nothing was emitted, and the client was silently dead.
 *
 * Runs against the gateway develop publishes; see GatewayContainer.ts to run it against a local build.
 */
const HOST_PORT = 58598
// The clienttest service as develop publishes it
const TEST_SERVICE_CRI = 'org.kinotic.continuum.gatewayserver.clienttest.ITestService'

describe('Sticky Session Gateway Restart Tests', () => {
    let container: StartedTestContainer
    const connectionInfo: ConnectionInfo = new ConnectionInfo()


    beforeAll(async () => {
        console.log(`Starting Continuum Gateway ${GATEWAY_IMAGE} for sticky session gateway restart test`)
        container = await startGateway('sticky-session-gateway-restart-test', HOST_PORT)
        connectionInfo.host = container.getHost()
        connectionInfo.port = HOST_PORT
        // Bounded, as a client should be: enough to outlast a restart, not forever
        connectionInfo.maxConnectionAttempts = 20
        // Sticky sessions on (the default) with static credentials: how real clients connect
        connectionInfo.connectHeaders = {login: 'guest', passcode: 'guest'}
        console.log(`Continuum Gateway running at ${connectionInfo.host}:${connectionInfo.port}`)
    }, 1000 * 60 * 10)

    afterAll(async () => {
        if (container) {
            await container.stop({timeout: 60000, remove: true, removeVolumes: true})
        }
    })

    it('fails the in-flight request on close, reports the refused reconnect, and lets the caller connect again',
       {timeout: 1000 * 60 * 6}, async () => {
        const continuum = new ContinuumSingleton()
        const connectedInfo: ConnectedInfo = await logFailure(continuum.connect(connectionInfo),
                                                              'Failed to connect to Continuum Gateway')
        validateConnectedInfo(connectedInfo)
        const originalSession = connectedInfo.sessionId

        const service: IServiceProxy = continuum.serviceProxy(TEST_SERVICE_CRI)
        expect(await service.invoke('testMethodWithString', ['before'])).toBe('Hello before')

        // The contract is that by the time fatalErrors emits the connection is already down, so the
        // caller may connect again right there. The recovery is done inside the handler to pin exactly that.
        const fatal: ContinuumError[] = []
        let activeWhenReported: boolean | null = null
        const recovered = new Promise<ConnectedInfo>((resolve, reject) => {
            continuum.eventBus.fatalErrors.subscribe(e => {
                fatal.push(e)
                activeWhenReported = continuum.eventBus.isConnectionActive()
                continuum.connect(connectionInfo).then(resolve, reject)
            })
        })

        // Stage 1: hold a request open on the instance, then take the instance away underneath it
        let inFlightOutcome: 'resolved' | 'rejected' | null = null
        let inFlightError: unknown = null
        const inFlight = service.invoke('testMethodWithDelay', ['in-flight', 30000])
            .then(() => { inFlightOutcome = 'resolved' },
                  e  => { inFlightOutcome = 'rejected'; inFlightError = e })
        await sleep(2000)

        console.log('Stopping the gateway underneath an established sticky session with a request in flight')
        const stopping = container.stop({timeout: 60000, remove: true, removeVolumes: true})

        await Promise.race([inFlight, sleep(90000)])
        expect(inFlightOutcome,
               'a request in flight when the socket closes must be failed then, not left waiting forever')
            .toBe('rejected')
        expect(inFlightError,
               'and failed with ConnectionLostError so a caller can tell it from an answer the server gave')
            .toBeInstanceOf(ConnectionLostError)
        await stopping

        // Stage 2: a fresh instance on the same address has no memory of our session. The reconnect
        // presents it, is refused, and that must be reported with the connection already down.
        await sleep(10000)
        console.log('Starting a fresh gateway on the same address')
        container = await startGateway('sticky-session-gateway-restart-test', HOST_PORT)

        // Stage 3: the caller decides how to recover - here, by connecting again with credentials from
        // inside the fatalErrors handler, which only works if the connection was really down by then
        const reconnectedInfo: ConnectedInfo = await logFailure(
            Promise.race([recovered, sleep(1000 * 150).then(() => Promise.reject(new Error('fatalErrors never emitted')))]),
            'Failed to connect again after the refusal')

        expect(fatal.length, 'a refused reconnect is reported on fatalErrors exactly once').toBe(1)
        expect(fatal[0], 'the refusal is the server saying no, typed so a caller can tell it from anything else')
            .toBeInstanceOf(ConnectionRefusedError)
        expect(activeWhenReported,
               'after a refused reconnect the connection must already be down when it is reported, not left silently half alive')
            .toBe(false)

        validateConnectedInfo(reconnectedInfo)
        expect(reconnectedInfo.sessionId, 'a fresh connect yields a fresh session').not.toBe(originalSession)
        expect(await service.invoke('testMethodWithString', ['after'])).toBe('Hello after')

        await continuum.disconnect()
    })
})

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function waitFor(predicate: () => boolean, budgetMs: number, intervalMs = 500): Promise<void> {
    const deadline = Date.now() + budgetMs
    while (Date.now() < deadline && !predicate()) {
        await sleep(intervalMs)
    }
}
