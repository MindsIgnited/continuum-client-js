import {afterAll, beforeAll, describe, expect, it} from 'vitest'
import {WebSocket} from 'ws'
import {
    AuthenticationError,
    ConnectedInfo,
    ConnectionInfo,
    ConnectionLostError,
    ContinuumError,
    ContinuumSingleton,
    IServiceProxy
} from '../src'
import {GenericContainer, StartedTestContainer, Wait} from 'testcontainers'
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
 *   2. when the reconnect is refused, the refusal is reported on fatalErrors as an AuthenticationError
 *      and the connection is already torn down - not left silently half alive
 *   3. the caller is free to connect again with credentials of its choosing, and it works
 *
 * Before this contract the refused reconnect deactivated the transport without running cleanup, so
 * pending requests waited forever, nothing was emitted, and the client was silently dead.
 *
 * Runs against a gateway built from continuum develop, which is what structures runs against. Build
 * it first from continuum-framework and give it the local tag:
 *   ./gradlew :continuum-gateway-server:bootBuildImage
 *   docker tag mindsignited/continuum-gateway-server:3.1.0-SNAPSHOT mindsignited/continuum-gateway-server:3.1.0-SNAPSHOT-local
 */
// A local-only tag on purpose: CI publishes 3.1.0-SNAPSHOT to Docker Hub, and a test that pulls would
// replace a local build carrying fixes not yet on develop with whatever CI last published
const GATEWAY_IMAGE = process.env.CONTINUUM_GATEWAY_IMAGE || 'mindsignited/continuum-gateway-server:3.1.0-SNAPSHOT-local'
const HOST_PORT = 58598
// The clienttest service as develop publishes it
const TEST_SERVICE_CRI = 'org.kinotic.continuum.gatewayserver.clienttest.ITestService'

describe('Sticky Session Gateway Restart Tests', () => {
    let container: StartedTestContainer
    const connectionInfo: ConnectionInfo = new ConnectionInfo()

    function startGateway(): Promise<StartedTestContainer> {
        return new GenericContainer(GATEWAY_IMAGE)
            .withExposedPorts({container: 58503, host: HOST_PORT})
            .withEnvironment({SPRING_PROFILES_ACTIVE: "clienttest"})
            .withWaitStrategy(Wait.forHttp('/', 58503))
            // The image is amd64; under emulation on arm64 hosts it boots well past the 60s default
            .withStartupTimeout(120000)
            .withName('sticky-session-gateway-restart-test')
            .start()
    }

    beforeAll(async () => {
        console.log(`Starting Continuum Gateway ${GATEWAY_IMAGE} for sticky session gateway restart test`)
        container = await startGateway()
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

        const fatal: ContinuumError[] = []
        continuum.eventBus.fatalErrors.subscribe(e => fatal.push(e))

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
        container = await startGateway()

        await waitFor(() => fatal.length > 0, 1000 * 150)
        expect(fatal.length, 'a refused reconnect must be reported on fatalErrors').toBeGreaterThan(0)
        expect(fatal[0], 'the refusal is an authentication failure and should be typed as one')
            .toBeInstanceOf(AuthenticationError)
        // fatalErrors is emitted once the teardown has completed, so this should already hold; the
        // short wait only guards the assertion against scheduling, not against the contract
        await waitFor(() => !continuum.eventBus.isConnectionActive(), 10000)
        expect(continuum.eventBus.isConnectionActive(),
               'after a refused reconnect the connection must be torn down, not left silently dead').toBe(false)

        // Stage 3: the caller decides how to recover - here, connect again with credentials
        const reconnectedInfo: ConnectedInfo = await logFailure(continuum.connect(connectionInfo),
                                                                'Failed to connect again after the refusal')
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
