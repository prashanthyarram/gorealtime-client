import { RealtimeError } from "./errors.js";
import {
  CONNECTION_TOKEN_HEADER,
  type Negotiation,
  type TransportName,
  negotiate,
  selectTransports,
  shouldFallback,
} from "./negotiation.js";
import { type Envelope, decodeServer, encodeClient, newFrameId, DEFAULT_MAX_PAYLOAD_BYTES } from "./protocol.js";
import { type RetryOptions, defaultRetry, retryDelay } from "./reconnect.js";
import { connectLongPolling } from "./transports/long-polling.js";
import { connectSSE } from "./transports/sse.js";
import type { TransportSession } from "./transports/types.js";
import { connectWebSocket } from "./transports/websocket.js";

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting" | "disconnecting";

export type LifecycleEvent = "connected" | "disconnected" | "reconnecting" | "reconnected" | "error" | "transport" | "fallback";

export interface FallbackInfo {
  from: TransportName;
  to: TransportName | null;
  status: number;
}

export interface RealtimeClientOptions {
  /** HTTP URL of the mount path, for example `https://api.example.com/realtime`. */
  url: string;
  /**
   * Returns the application's access token. The value is sent as
   * `Authorization: Bearer <token>` on negotiate, SSE, long polling, and POST.
   * It is not placed in a URL. A value that already starts with `Bearer ` is used as-is.
   * The browser cannot attach this header to a WebSocket, so WebSocket is skipped
   * unless {@link RealtimeClientOptions.attemptWebSocketWithBearer} is set.
   */
  accessTokenFactory?: () => string | Promise<string | undefined>;
  /**
   * Try WebSocket even when {@link RealtimeClientOptions.accessTokenFactory} is set.
   * The socket still cannot send the bearer token. Use this when cookies authenticate the upgrade.
   */
  attemptWebSocketWithBearer?: boolean;
  /** Fetch credentials mode. Default `same-origin`, so cookies are sent to the same site. */
  credentials?: RequestCredentials;
  /** Extra headers for HTTP requests. Ignored by the browser WebSocket handshake. */
  headers?: Record<string, string> | (() => Record<string, string> | Promise<Record<string, string>>);
  /** Preferred order. The server's offered list is the other filter. */
  transports?: TransportName[];
  /** When true, an unexpected drop starts a new negotiate and a new connection. Default false. */
  automaticReconnect?: boolean;
  reconnect?: RetryOptions;
  /** Largest JSON payload, in bytes. Default 32 KiB, matching the Go server. */
  maxPayloadBytes?: number;
  /** How long to wait for `hello` after a transport opens. Default 10 seconds. */
  helloTimeoutMs?: number;
  fetch?: typeof fetch;
  webSocketFactory?: (url: string) => WebSocket;
  /** Test hook. Returns a number in [0, 1). */
  random?: () => number;
}

type Handler = (payload: unknown, envelope: Envelope) => void;
type LifeHandler = (info?: unknown) => void;

const DEFAULT_TRANSPORTS: TransportName[] = ["WebSockets", "ServerSentEvents", "LongPolling"];

/**
 * Client for one gorealtime server.
 *
 * `connect` and `send` reject on failure. Lifecycle problems are also reported
 * to `onLifecycle` listeners. One listener throwing does not stop the others.
 * A message that failed to send is not sent again after a reconnect.
 */
export class RealtimeClient {
  private readonly options: RealtimeClientOptions;
  private stateValue: ConnectionState = "disconnected";
  private session: TransportSession | null = null;
  private transportValue: TransportName | null = null;
  private connectionIdValue: string | null = null;
  private generation = 0;
  private userClosed = false;
  private closing = false;
  private sawHello = false;
  private connectPromise: Promise<void> | null = null;
  private abort: AbortController | null = null;
  private sleeper: { id: ReturnType<typeof setTimeout>; resolve: () => void } | null = null;
  private readonly handlers = new Map<string, Set<Handler>>();
  private readonly lifecycle = new Map<LifecycleEvent, Set<LifeHandler>>();
  private hello: { promise: Promise<void>; resolve: () => void; reject: (error: Error) => void } | null = null;
  private reconnecting = false;
  private lost = false;
  private epoch = 0;

  constructor(options: RealtimeClientOptions) {
    if (!options.url) {
      throw new RealtimeError("lifecycle", "url is required");
    }
    this.options = options;
  }

  /** Current lifecycle state. */
  get state(): ConnectionState {
    return this.stateValue;
  }

  /** Transport that carried the current connection, or null when none is up. */
  get transport(): TransportName | null {
    return this.transportValue;
  }

  /** Connection id from the server `hello` frame. */
  get connectionId(): string | null {
    return this.connectionIdValue;
  }

  /**
   * Registers a handler for server `event` frames.
   * Returns a function that removes that handler.
   * Handlers for one event run in registration order.
   */
  on<T = unknown>(event: string, handler: (payload: T, envelope: Envelope) => void): () => void {
    const wrapped: Handler = (payload, envelope) => handler(payload as T, envelope);
    let set = this.handlers.get(event);
    if (!set) {
      set = new Set();
      this.handlers.set(event, set);
    }
    set.add(wrapped);
    return () => this.off(event, wrapped);
  }

  /** Removes one handler, or every handler for `event` when `handler` is omitted. */
  off(event: string, handler?: Handler): void {
    const set = this.handlers.get(event);
    if (!set) {
      return;
    }
    if (!handler) {
      this.handlers.delete(event);
      return;
    }
    set.delete(handler);
    if (set.size === 0) {
      this.handlers.delete(event);
    }
  }

  /**
   * Lifecycle listeners run synchronously, in registration order.
   * `connected` fires each time the state becomes connected.
   * `reconnected` fires only when that followed `reconnecting`.
   * `error` receives a {@link RealtimeError}.
   * `transport` receives the selected transport name.
   * `fallback` receives {@link FallbackInfo}.
   */
  onLifecycle(event: LifecycleEvent, handler: LifeHandler): () => void {
    let set = this.lifecycle.get(event);
    if (!set) {
      set = new Set();
      this.lifecycle.set(event, set);
    }
    set.add(handler);
    return () => set.delete(handler);
  }

  /** Opens a connection. A second call while one is in progress returns the same promise. */
  connect(): Promise<void> {
    if (this.connectPromise && (this.stateValue === "connecting" || this.stateValue === "connected" || this.stateValue === "reconnecting")) {
      return this.connectPromise;
    }
    this.userClosed = false;
    this.closing = false;
    this.reconnecting = false;
    this.lost = false;
    const gen = ++this.generation;
    this.setState("connecting");
    this.connectPromise = this.establish(gen)
      .then(() => {
        if (this.generation !== gen) {
          return;
        }
        this.setState("connected");
        this.emit("connected");
      })
      .catch((error: unknown) => {
        if (this.generation === gen && !this.userClosed) {
          this.setState("disconnected");
        }
        throw error;
      });
    return this.connectPromise;
  }

  /** Stops the connection and any reconnect loop. Safe to call more than once. */
  async disconnect(): Promise<void> {
    this.userClosed = true;
    this.closing = true;
    this.generation += 1;
    this.wake();
    this.abort?.abort();
    this.abort = null;
    const session = this.session;
    this.session = null;
    this.transportValue = null;
    if (this.stateValue === "disconnected") {
      this.closing = false;
      this.connectPromise = null;
      return;
    }
    this.setState("disconnecting");
    session?.close();
    this.sawHello = false;
    this.setState("disconnected");
    this.emit("disconnected");
    this.closing = false;
    this.connectPromise = null;
  }

  /** Sends one event. Rejects when the client is not connected or the write fails. Nothing is queued for a later reconnect. */
  async send(event: string, payload?: unknown): Promise<void> {
    if (this.stateValue !== "connected" || !this.session) {
      throw new RealtimeError("disconnected", "client is not connected");
    }
    const text = encodeClient(
      { v: 1, type: "event", event, id: newFrameId(), payload },
      this.options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES,
    );
    try {
      await this.session.send(text);
    } catch (cause) {
      if (cause instanceof RealtimeError) {
        throw cause;
      }
      throw new RealtimeError("send_failed", "send failed", { cause });
    }
  }

  /**
   * Sends an `ack` for a frame id. An ack means this client claims to have seen that id.
   * It is not a durable-delivery receipt, and the server does not replay from it.
   */
  async ack(correlationId: string): Promise<void> {
    if (this.stateValue !== "connected" || !this.session) {
      throw new RealtimeError("disconnected", "client is not connected");
    }
    const text = encodeClient(
      { v: 1, type: "ack", id: newFrameId(), corr: correlationId },
      this.options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES,
    );
    await this.session.send(text);
  }

  private async establish(gen: number): Promise<void> {
    this.abort?.abort();
    const controller = new AbortController();
    this.abort = controller;
    this.sawHello = false;
    this.session?.close();
    this.session = null;
    const headers = await this.buildHeaders();
    if (gen !== this.generation || controller.signal.aborted) {
      throw new RealtimeError("cancelled", "connection cancelled");
    }
    const negotiation = await negotiate(this.endpoint(), {
      headers,
      credentials: this.options.credentials ?? "same-origin",
      fetchImpl: this.fetch(),
      signal: controller.signal,
    });
    const order = this.candidateTransports(negotiation);
    if (order.length === 0) {
      throw new RealtimeError("transport_failed", "no offered transport can be attempted");
    }
    let last: RealtimeError | null = null;
    for (let i = 0; i < order.length; i += 1) {
      const name = order[i];
      if (last) {
        this.emit("fallback", { from: order[i - 1], to: name, status: last.status ?? 0 } satisfies FallbackInfo);
      }
      try {
        await this.openTransport(name, negotiation, controller.signal, gen);
        this.transportValue = name;
        this.emit("transport", name);
        return;
      } catch (cause) {
        const error = cause instanceof RealtimeError ? cause : new RealtimeError("transport_failed", "transport failed", { cause });
        if (controller.signal.aborted || gen !== this.generation || error.code === "cancelled") {
          throw error.code === "cancelled" ? error : new RealtimeError("cancelled", "connection cancelled");
        }
        last = error;
        const status = error.status ?? 0;
        const more = i < order.length - 1 && shouldFallback(status);
        if (!more) {
          if (i < order.length - 1) {
            this.emit("fallback", { from: name, to: null, status } satisfies FallbackInfo);
          }
          throw error;
        }
      }
    }
    throw last ?? new RealtimeError("transport_failed", "no transport succeeded");
  }

  private candidateTransports(negotiation: Negotiation): TransportName[] {
    const preference = this.options.transports ?? DEFAULT_TRANSPORTS;
    let names = selectTransports(preference, negotiation.availableTransports);
    if (this.options.accessTokenFactory && !this.options.attemptWebSocketWithBearer) {
      names = names.filter((name) => name !== "WebSockets");
    }
    return names;
  }

  private async openTransport(name: TransportName, negotiation: Negotiation, signal: AbortSignal, gen: number): Promise<void> {
    const child = new AbortController();
    const onAbort = () => child.abort();
    signal.addEventListener("abort", onAbort);
    const hello = defer();
    this.hello = hello;
    const epoch = ++this.epoch;
    const args = {
      httpURL: this.endpoint(),
      token: negotiation.connectionToken,
      credentials: this.options.credentials ?? "same-origin",
      signal: child.signal,
      fetchImpl: this.fetch(),
      webSocketFactory: this.options.webSocketFactory ?? ((url: string) => new WebSocket(url)),
      headers: () => this.buildHeaders(),
      onText: (text: string) => this.handleText(text, gen, epoch),
      onDown: (error: RealtimeError) => this.handleDown(error, gen, epoch),
    };
    try {
      const session =
        name === "WebSockets"
          ? await connectWebSocket(args)
          : name === "ServerSentEvents"
            ? await connectSSE(args)
            : await connectLongPolling(args);
      if (gen !== this.generation) {
        session.close();
        throw new RealtimeError("cancelled", "connection cancelled");
      }
      this.session = session;
      await withTimeout(hello.promise, this.options.helloTimeoutMs ?? 10_000);
    } catch (cause) {
      this.session?.close();
      this.session = null;
      child.abort();
      if (cause instanceof RealtimeError) {
        throw cause;
      }
      throw new RealtimeError("transport_failed", "transport failed", { status: 0, cause });
    } finally {
      signal.removeEventListener("abort", onAbort);
      if (!this.sawHello) {
        this.session?.close();
        this.session = null;
        child.abort();
      }
    }
  }

  private handleText(text: string, gen: number, epoch: number): void {
    if (gen !== this.generation || epoch !== this.epoch) {
      return;
    }
    let env: Envelope;
    try {
      env = decodeServer(text);
    } catch (cause) {
      this.report(cause);
      if (!this.sawHello) {
        this.hello?.reject(cause instanceof RealtimeError ? cause : new RealtimeError("protocol", "bad frame"));
      }
      return;
    }
    if (!this.sawHello) {
      if (env.type !== "hello") {
        const error = new RealtimeError("protocol", "first frame was not hello");
        this.hello?.reject(error);
        return;
      }
      const payload = env.payload as { connectionId: string };
      this.connectionIdValue = payload.connectionId;
      this.sawHello = true;
      this.hello?.resolve();
      return;
    }
    if (env.type === "event" && env.event) {
      this.dispatch(env);
      return;
    }
    if (env.type === "error") {
      this.report(new RealtimeError("server", env.error?.message || "server error"));
      return;
    }
    if (env.type === "bye") {
      this.handleDown(new RealtimeError("disconnected", "server closed the connection"), gen, epoch);
      return;
    }
    if (env.type !== "hello") {
      this.report(new RealtimeError("protocol", "unknown frame type"));
    }
  }

  private dispatch(env: Envelope): void {
    const set = this.handlers.get(env.event ?? "");
    if (!set) {
      return;
    }
    for (const handler of set) {
      try {
        handler(env.payload, env);
      } catch (cause) {
        this.report(new RealtimeError("lifecycle", "event handler threw", { cause }));
      }
    }
  }

  private handleDown(error: RealtimeError, gen: number, epoch: number): void {
    if (gen !== this.generation || epoch !== this.epoch || this.closing || this.userClosed) {
      return;
    }
    if (!this.sawHello) {
      this.hello?.reject(error);
      return;
    }
    this.lost = true;
    this.sawHello = false;
    this.session?.close();
    this.session = null;
    this.transportValue = null;
    this.emit("disconnected");
    if (this.reconnecting) {
      return;
    }
    if (!this.options.automaticReconnect) {
      this.setState("disconnected");
      this.report(error);
      this.connectPromise = null;
      return;
    }
    this.reconnecting = true;
    void this.reconnect(gen, error);
  }

  private async reconnect(gen: number, cause: RealtimeError): Promise<void> {
    const cfg = { ...defaultRetry, ...this.options.reconnect };
    this.setState("reconnecting");
    this.emit("reconnecting");
    try {
      for (let attempt = 0; attempt < cfg.maxRetries; attempt += 1) {
        if (this.userClosed || gen !== this.generation) {
          return;
        }
        const delay = retryDelay(attempt, cfg, this.options.random ?? Math.random);
        await this.sleep(delay);
        if (this.userClosed || gen !== this.generation) {
          return;
        }
        try {
          this.lost = false;
          await this.establish(gen);
          if (this.lost || this.userClosed || gen !== this.generation) {
            continue;
          }
          this.setState("connected");
          this.emit("connected");
          this.emit("reconnected");
          return;
        } catch (error) {
          const wrapped = error instanceof RealtimeError ? error : new RealtimeError("reconnect_failed", "reconnect failed", { cause: error });
          if (wrapped.code === "cancelled" || this.userClosed) {
            return;
          }
          if (!shouldFallback(wrapped.status ?? 0) && wrapped.code !== "transport_failed" && wrapped.code !== "negotiation_failed") {
            this.setState("disconnected");
            this.report(wrapped);
            this.connectPromise = null;
            return;
          }
          cause = wrapped;
        }
      }
      if (gen === this.generation && !this.userClosed) {
        this.setState("disconnected");
        this.report(new RealtimeError("reconnect_failed", "reconnect attempts exhausted", { cause }));
        this.connectPromise = null;
      }
    } finally {
      this.reconnecting = false;
    }
  }

  private async buildHeaders(): Promise<Headers> {
    const headers = new Headers();
    const extra = typeof this.options.headers === "function" ? await this.options.headers() : this.options.headers;
    if (extra) {
      for (const [key, value] of Object.entries(extra)) {
        headers.set(key, value);
      }
    }
    if (this.options.accessTokenFactory) {
      const token = await this.options.accessTokenFactory();
      if (token) {
        headers.set("Authorization", token.startsWith("Bearer ") ? token : `Bearer ${token}`);
      }
    }
    headers.delete(CONNECTION_TOKEN_HEADER);
    return headers;
  }

  private endpoint(): string {
    return this.options.url.replace(/\/+$/, "");
  }

  private fetch(): typeof fetch {
    return this.options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  private setState(state: ConnectionState): void {
    this.stateValue = state;
  }

  private emit(event: LifecycleEvent, info?: unknown): void {
    const set = this.lifecycle.get(event);
    if (!set) {
      return;
    }
    for (const handler of set) {
      try {
        handler(info);
      } catch (cause) {
        if (event !== "error") {
          this.report(new RealtimeError("lifecycle", "lifecycle handler threw", { cause }));
        }
      }
    }
  }

  private report(cause: unknown): void {
    const error = cause instanceof RealtimeError ? cause : new RealtimeError("lifecycle", "client error", { cause });
    this.emit("error", error);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const id = setTimeout(() => {
        this.sleeper = null;
        resolve();
      }, ms);
      this.sleeper = { id, resolve };
    });
  }

  private wake(): void {
    if (!this.sleeper) {
      return;
    }
    clearTimeout(this.sleeper.id);
    const resolve = this.sleeper.resolve;
    this.sleeper = null;
    resolve();
  }
}

function defer(): { promise: Promise<void>; resolve: () => void; reject: (error: Error) => void } {
  let resolve: () => void = () => undefined;
  let reject: (error: Error) => void = () => undefined;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function withTimeout(promise: Promise<void>, ms: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new RealtimeError("transport_failed", "hello timed out", { status: 0 })), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}
