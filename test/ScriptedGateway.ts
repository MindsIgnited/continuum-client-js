import {AddressInfo} from 'node:net'
import {RawData, WebSocket as ServerSocket, WebSocketServer} from 'ws'
import {v4 as uuidv4} from 'uuid'

/**
 * A STOMP-over-WebSocket server that speaks just enough of the protocol for the client to connect,
 * subscribe, send and disconnect, and that a test drives frame by frame: accept or refuse a CONNECT,
 * answer a CONNECTED with whatever headers it likes, ignore a DISCONNECT, drop the socket. It records
 * every frame each connection sends so a test can assert on what the client actually put on the wire.
 *
 * It exists for the connection contract: the situations a real gateway cannot be made to produce on
 * demand - a peer that never answers, a malformed CONNECTED frame, a session that is not remembered -
 * are exactly the ones that decide whether a client hangs.
 */

export interface Frame {
    command: string
    headers: Record<string, string>
    body: string
}

export type ConnectDecision =
    /** A normal CONNECTED carrying connected-info, with the session id given or a fresh one */
    | { accept: true, sessionId?: string }
    /** An ERROR frame with this message, then the socket is closed - what a refused connect looks like */
    | { refuse: string }
    /** A CONNECTED with exactly these headers and nothing added, for the malformed cases */
    | { connectedHeaders: Record<string, string> }

export class GatewayConnection {

    public readonly frames: Frame[] = []
    /** subscription id -> destination, as the client has SUBSCRIBEd them */
    public readonly subscriptions = new Map<string, string>()
    public sessionId: string | null = null
    public readonly closed: Promise<void>
    private waiters: { predicate: (frame: Frame) => boolean, resolve: (frame: Frame) => void }[] = []

    constructor(public readonly socket: ServerSocket, public readonly index: number) {
        this.closed = new Promise(resolve => socket.once('close', () => resolve()))
    }

    /** Resolves with the first frame, already received or still to come, that the predicate accepts */
    public waitForFrame(predicate: (frame: Frame) => boolean, timeoutMs: number = 10000, what: string = 'frame'): Promise<Frame> {
        const already = this.frames.find(predicate)
        if (already) {
            return Promise.resolve(already)
        }
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.waiters = this.waiters.filter(w => w.resolve !== wrapped)
                reject(new Error(`connection #${this.index} did not receive ${what} within ${timeoutMs}ms; received ${this.frames.map(f => f.command).join(', ') || 'nothing'}`))
            }, timeoutMs)
            const wrapped = (frame: Frame): void => {
                clearTimeout(timer)
                resolve(frame)
            }
            this.waiters.push({predicate, resolve: wrapped})
        })
    }

    public send(command: string, headers: Record<string, string>, body: string = ''): void {
        if (this.socket.readyState !== ServerSocket.OPEN) {
            return
        }
        // STOMP 1.2 escapes header values everywhere except the CONNECT and CONNECTED frames
        const escape = command === 'CONNECTED' ? (v: string) => v : escapeHeaderValue
        const headerLines = Object.entries(headers).map(([k, v]) => `${k}:${escape(v)}`).join('\n')
        this.socket.send(`${command}\n${headerLines}\n\n${body}\0`)
    }

    /** A MESSAGE to a destination the client has subscribed to */
    public sendMessage(destination: string, headers: Record<string, string>, body: string = ''): void {
        const subscription = [...this.subscriptions.entries()].find(([, d]) => d === destination)?.[0]
        if (subscription == null) {
            throw new Error(`client has no subscription to ${destination}`)
        }
        this.send('MESSAGE', {destination, subscription, 'message-id': uuidv4(), ...headers}, body)
    }

    /** What a server does when it is done with a client: an ERROR frame, then the socket is closed */
    public sendError(message: string): void {
        this.send('ERROR', {message, 'content-type': 'text/plain'}, message)
        this.socket.close()
    }

    /** The socket goes away with no DISCONNECT and no close handshake - the instance died */
    public drop(): void {
        this.socket.terminate()
    }

    /** @internal */
    public receive(frame: Frame): void {
        this.frames.push(frame)
        const waiters = this.waiters
        this.waiters = []
        for (const waiter of waiters) {
            if (waiter.predicate(frame)) {
                waiter.resolve(frame)
            } else {
                this.waiters.push(waiter)
            }
        }
    }
}

export class ScriptedGateway {

    public readonly connections: GatewayConnection[] = []
    /** Sessions this gateway remembers; clear it to become an instance with no memory of any session */
    public readonly sessions = new Set<string>()
    /** Whether a DISCONNECT gets its RECEIPT. A server that never answers is the half-open peer */
    public ackDisconnect: boolean = true
    /**
     * How a CONNECT is answered. The default is the gateway's sticky session logic: a session header is
     * accepted only if the session is remembered, anything else gets a fresh session.
     */
    public onConnect: (headers: Record<string, string>, connection: GatewayConnection) => ConnectDecision = (headers) => {
        const presented = headers['session']
        if (presented != null) {
            return this.sessions.has(presented)
                ? {accept: true, sessionId: presented}
                : {refuse: 'Could not authenticate with the given Session id'}
        }
        return {accept: true}
    }
    private connectionWaiters: { index: number, resolve: (c: GatewayConnection) => void }[] = []

    private constructor(private readonly server: WebSocketServer) {
        server.on('connection', socket => this.accept(socket))
    }

    public static start(): Promise<ScriptedGateway> {
        return new Promise((resolve, reject) => {
            const server = new WebSocketServer({host: '127.0.0.1', port: 0, path: '/v1'})
            server.once('listening', () => resolve(new ScriptedGateway(server)))
            server.once('error', reject)
        })
    }

    public get host(): string {
        return '127.0.0.1'
    }

    public get port(): number {
        return (this.server.address() as AddressInfo).port
    }

    /** The nth connection the client makes (0-based), whether it has arrived yet or not */
    public waitForConnection(index: number, timeoutMs: number = 20000): Promise<GatewayConnection> {
        if (this.connections[index]) {
            return Promise.resolve(this.connections[index])
        }
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.connectionWaiters = this.connectionWaiters.filter(w => w.resolve !== wrapped)
                reject(new Error(`connection #${index} did not arrive within ${timeoutMs}ms; ${this.connections.length} so far`))
            }, timeoutMs)
            const wrapped = (c: GatewayConnection): void => {
                clearTimeout(timer)
                resolve(c)
            }
            this.connectionWaiters.push({index, resolve: wrapped})
        })
    }

    public stop(): Promise<void> {
        for (const connection of this.connections) {
            connection.socket.terminate()
        }
        return new Promise(resolve => this.server.close(() => resolve()))
    }

    private accept(socket: ServerSocket): void {
        const connection = new GatewayConnection(socket, this.connections.length)
        this.connections.push(connection)
        socket.on('message', (data: RawData) => {
            for (const frame of parseFrames(data)) {
                connection.receive(frame)
                this.handle(connection, frame)
            }
        })
        const waiters = this.connectionWaiters
        this.connectionWaiters = []
        for (const waiter of waiters) {
            if (waiter.index === connection.index) {
                waiter.resolve(connection)
            } else {
                this.connectionWaiters.push(waiter)
            }
        }
    }

    private handle(connection: GatewayConnection, frame: Frame): void {
        switch (frame.command) {
            case 'CONNECT':
            case 'STOMP': {
                const decision = this.onConnect(frame.headers, connection)
                if ('refuse' in decision) {
                    connection.sendError(decision.refuse)
                } else if ('connectedHeaders' in decision) {
                    connection.send('CONNECTED', decision.connectedHeaders)
                } else {
                    const sessionId = decision.sessionId ?? uuidv4()
                    this.sessions.add(sessionId)
                    connection.sessionId = sessionId
                    const connectedInfo = {
                        sessionId,
                        replyToId: frame.headers['reply-to-id'] ?? uuidv4(),
                        participant: {id: frame.headers['login'] ?? 'session', roles: ['ADMIN'], metadata: {}}
                    }
                    connection.send('CONNECTED', {
                        version: '1.2',
                        'heart-beat': '0,0',
                        'connected-info': JSON.stringify(connectedInfo)
                    })
                }
                break
            }
            case 'SUBSCRIBE':
                connection.subscriptions.set(frame.headers['id'], frame.headers['destination'])
                break
            case 'UNSUBSCRIBE':
                connection.subscriptions.delete(frame.headers['id'])
                break
            case 'DISCONNECT':
                if (this.ackDisconnect && frame.headers['receipt']) {
                    connection.send('RECEIPT', {'receipt-id': frame.headers['receipt']})
                }
                break
            default:
                break
        }
    }
}

/** A message may hold heartbeats (bare newlines) and at most one frame; both are handled */
function parseFrames(data: RawData): Frame[] {
    const text = typeof data === 'string' ? data : new TextDecoder().decode(data as Buffer)
    const start = text.search(/[^\r\n]/)
    if (start < 0) {
        return []
    }
    const raw = text.substring(start)
    const separator = raw.indexOf('\n\n')
    const head = separator < 0 ? raw : raw.substring(0, separator)
    let body = separator < 0 ? '' : raw.substring(separator + 2)
    const terminator = body.indexOf('\0')
    if (terminator >= 0) {
        body = body.substring(0, terminator)
    }
    const [commandLine, ...headerLines] = head.split('\n')
    const command = commandLine.replace(/\r$/, '')
    // STOMP 1.2 escapes header values everywhere except the CONNECT and CONNECTED frames
    const unescape = command === 'CONNECT' || command === 'STOMP' ? (v: string) => v : unescapeHeaderValue
    const headers: Record<string, string> = {}
    for (const line of headerLines) {
        const colon = line.indexOf(':')
        if (colon > 0 && !(line.substring(0, colon) in headers)) {
            headers[line.substring(0, colon)] = unescape(line.substring(colon + 1).replace(/\r$/, ''))
        }
    }
    return [{command, headers, body}]
}

function escapeHeaderValue(value: string): string {
    return value.replace(/\\/g, '\\\\').replace(/\r/g, '\\r').replace(/\n/g, '\\n').replace(/:/g, '\\c')
}

function unescapeHeaderValue(value: string): string {
    return value.replace(/\\(.)/g, (_, c: string) => ({r: '\r', n: '\n', c: ':', '\\': '\\'}[c] ?? '\\' + c))
}
