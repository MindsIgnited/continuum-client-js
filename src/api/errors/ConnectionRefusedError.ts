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

import {IFrame} from '@stomp/rx-stomp'
import {ContinuumError} from './ContinuumError'

/**
 * Raised when the server ends the connection with a STOMP ERROR frame.
 *
 * That happens when a connect is refused - bad credentials, or a reconnect presenting a session the
 * server no longer has - and when the server rejects something sent over an established connection.
 * In every case the connection is already closed by the time this is seen; it is the caller's
 * decision whether to call connect() again, and with what.
 *
 * The message is the server's own. The frame's headers and body are kept as sent, for callers that
 * want more than the message.
 */
export class ConnectionRefusedError extends ContinuumError {

    public readonly headers: Readonly<Record<string, string>>
    public readonly body: string

    constructor(frame: IFrame) {
        super(frame.headers['message'] ?? 'Connection refused by server');
        Object.setPrototypeOf(this, ConnectionRefusedError.prototype);
        this.headers = {...frame.headers}
        this.body = frame.body
    }
}
