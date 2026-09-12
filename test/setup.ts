// @ts-ignore
import path from 'node:path'
import {GenericContainer, PullPolicy, StartedTestContainer, Wait} from 'testcontainers'
import {TestProject} from 'vitest/node.js'

let container: StartedTestContainer

// Run once before all tests
export async function setup(project: TestProject) {
    // @ts-ignore
    if(import.meta.env.VITE_USE_GATEWAY_DOCKER === 'true') {
        console.log('Starting Continuum Gateway')

        container = await new GenericContainer((process.env.CONTINUUM_GATEWAY_IMAGE || 'mindignited/continuum-gateway-server:latest'))
            .withExposedPorts(58503)
            .withEnvironment({SPRING_PROFILES_ACTIVE: "clienttest"})
            .withPullPolicy(process.env.CONTINUUM_GATEWAY_IMAGE ? PullPolicy.defaultPolicy() : PullPolicy.alwaysPull())
            .withWaitStrategy(Wait.forHttp('/', 58503))
            .start()

        // @ts-ignore
        project.provide('CONTINUUM_HOST', container.getHost())
        // @ts-ignore
        project.provide('CONTINUUM_PORT', container.getMappedPort(58503))

        console.log(`Continuum Gateway started at ${container.getHost()}:${container.getMappedPort(58503)} `)
    }else{
        // @ts-ignore
        project.provide('CONTINUUM_HOST', '127.0.0.1')
        // @ts-ignore
        project.provide('CONTINUUM_PORT', 58503)
        console.log('Skipping Continuum Gateway start because VITE_USE_GATEWAY_DOCKER is false')
    }
}

// Run once after all tests
export async function teardown() {
    // Nothing was started when VITE_USE_GATEWAY_DOCKER was not true, and tearing down what was
    // never started turned every such run into a failed exit regardless of its tests
    if (!container) {
        return
    }
    console.log('Shutting down Continuum Gateway...')
    await container.stop()
    console.log('Continuum Gateway shut down.')
}



