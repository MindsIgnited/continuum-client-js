import {GenericContainer, PullPolicy, StartedTestContainer, Wait} from 'testcontainers'

/**
 * The gateway the tests run against: the public snapshot continuum CI publishes on every push to develop,
 * unless CONTINUUM_GATEWAY_IMAGE names another one. Every test that starts a gateway starts it here, so
 * they all run the same build under the same rules.
 */
export const GATEWAY_IMAGE = process.env.CONTINUUM_GATEWAY_IMAGE || 'mindsignited/continuum-gateway-server:3.1.0-SNAPSHOT'

/**
 * @param name to give the container, for tests that need to find it again; omitted for the shared gateway
 * @param hostPort fixed host port to publish 58503 on, so a gateway restarted mid-test keeps its address;
 *                 omitted to let Docker pick one
 */
export function startGateway(name?: string, hostPort?: number): Promise<StartedTestContainer> {
    let container = new GenericContainer(GATEWAY_IMAGE)
        .withExposedPorts(hostPort ? {container: 58503, host: hostPort} : 58503)
        .withEnvironment({SPRING_PROFILES_ACTIVE: 'clienttest'})
        // The snapshot tag moves, so it is pulled every run. A supplied image is never pulled: a local
        // build must not be replaced by whatever CI last published
        .withPullPolicy(process.env.CONTINUUM_GATEWAY_IMAGE ? PullPolicy.defaultPolicy() : PullPolicy.alwaysPull())
        .withWaitStrategy(Wait.forHttp('/', 58503))
        // The image is amd64; under emulation on arm64 hosts it boots well past the 60s default
        .withStartupTimeout(120000)
    if (name) {
        container = container.withName(name)
    }
    return container.start()
}
