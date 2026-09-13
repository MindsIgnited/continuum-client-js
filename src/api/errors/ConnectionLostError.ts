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

import {ContinuumError} from './ContinuumError'

/**
 * Raised for a request that was in flight when the connection to the server was lost.
 *
 * The server that held the request can no longer deliver its reply, so the request is failed rather
 * than left waiting for a reply that will never arrive. It says only that this particular request is
 * gone: whether the connection is reconnecting, was closed by the server, or was disconnected on
 * purpose is not part of it, though the message says which.
 *
 * It is distinct from an application error returned by the server so that a caller can tell "the
 * server told me no" from "the server never got to answer". Before retrying, look at {@link cause}:
 * when the server closed the connection with an ERROR, that error is here, and one of the requests
 * failed by the close may be what it refused - the gateway ends the whole connection over a single
 * SEND it will not accept, so retrying that one ends the next connection too. A cause of undefined
 * means the socket dropped or the caller disconnected; retry then if the operation is idempotent.
 */
export class ConnectionLostError extends ContinuumError {

    /** The error the connection closed with, when the server closed it; undefined for a socket loss or a requested disconnect */
    public readonly cause: ContinuumError | undefined

    constructor(message: string, cause?: ContinuumError) {
        super(message);
        Object.setPrototypeOf(this, ConnectionLostError.prototype);
        this.name = 'ConnectionLostError'
        this.cause = cause
    }
}
