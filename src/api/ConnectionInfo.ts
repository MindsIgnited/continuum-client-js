/*
 *
 * Copyright 2008-2021 Kinotic and the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

/**
 * ConnectHeaders to use during connection to the continuum server
 * These headers will be sent as part of the STOMP CONNECT frame
 * This is typically used for authentication information, but any data can be sent
 */
export class ConnectHeaders {
    [key: string]: string
}

export class ServerInfo {
    host!: string
    port?: number | null
    useSSL?: boolean | null
}

/**
 * ConnectionInfo provides the information needed to connect to the continuum server
 */
export class ConnectionInfo extends ServerInfo {
    /**
     * The headers to send during the connection to the continuum server.
     * If a function is provided, it will be called to get the headers each time a connection is attempted.
     * This is useful for providing dynamic headers, such as a JWT token that expires.
     *
     * With sticky sessions (the default) a reconnect presents only the session the server issued, so these
     * are used on the initial connect alone and are never held by the library. If that session is refused
     * the connection is closed and reported on {@link IEventBus#fatalErrors}; calling connect() again is
     * how it is re-established, with whatever credentials are right at that point.
     * With {@link disableStickySession} every reconnect authenticates afresh with these headers - a function
     * is called again each time.
     */
    connectHeaders?: ConnectHeaders | (() => Promise<ConnectHeaders>)

    /**
     * The maximum number of connection attempts to make, initially and on each reconnect.
     * If the limit is reached the connection is closed: before the initial connect succeeds this rejects
     * the {@link IEventBus#connect} promise, after that it is reported on {@link IEventBus#fatalErrors}.
     * Set to 0, undefined, or null to try forever
     */
    maxConnectionAttempts?: number | null

    /**
     * How long a single connection attempt may take before it is abandoned and counted as a failed
     * attempt: the {@link connectHeaders} function, if there is one, and then the socket and STOMP
     * handshake up to the CONNECTED frame, each bounded by this. It covers a token endpoint that never
     * answers as well as a peer that accepts the socket and then says nothing.
     * Default 10 seconds; set higher for slow networks. Zero or less means the default, not no bound.
     */
    connectTimeoutMs?: number | null

    /**
     * If true, the session will not be kept alive after the connection is established and then disrupted.
     * If false, the session will be kept alive after the connection is established and then disrupted, for a period of time.
     */
    disableStickySession?: boolean | null

}


