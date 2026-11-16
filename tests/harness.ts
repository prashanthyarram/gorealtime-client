import type { TransportName } from "../dist/index.js";

export interface FakeSocket {
  url: string;
  readyState: number;
  sent: string[];
  opened: boolean;
  closed: boolean;
  addEventListener(type: string, fn: (event: { data?: string }) => void): void;
  removeEventListener(type: string, fn: (event: { data?: string }) => void): void;
  send(data: string): void;
  close(): void;
  open(): void;
  serverMessage(text: string): void;
  failHandshake(): void;
}

export function installSockets(): { sockets: FakeSocket[]; factory: (url: string) => FakeSocket } {
  const sockets: FakeSocket[] = [];
  return {
    sockets,
    factory(url: string) {
      const listeners = new Map<string, Set<(event: { data?: string }) => void>>();
      const socket: FakeSocket = {
        url,
        readyState: 0,
        sent: [],
        opened: false,
        closed: false,
        addEventListener(type, fn) {
          let set = listeners.get(type);
          if (!set) {
            set = new Set();
            listeners.set(type, set);
          }
          set.add(fn);
        },
        removeEventListener(type, fn) {
          listeners.get(type)?.delete(fn);
        },
        send(data) {
          socket.sent.push(data);
        },
        close() {
          if (socket.closed) {
            return;
          }
          socket.closed = true;
          socket.readyState = 3;
          const closeListeners = [...(listeners.get("close") ?? [])];
          listeners.clear();
          for (const fn of closeListeners) {
            fn({});
          }
        },
        open() {
          if (socket.opened || socket.closed) {
            return;
          }
          socket.opened = true;
          socket.readyState = 1;
          for (const fn of listeners.get("open") ?? []) {
            fn({});
          }
        },
        serverMessage(text) {
          for (const fn of listeners.get("message") ?? []) {
            fn({ data: text });
          }
        },
        failHandshake() {
          for (const fn of [...(listeners.get("error") ?? [])]) {
            fn({});
          }
          socket.close();
        },
      };
      sockets.push(socket);
      return socket;
    },
  };
}

export interface RouteResult {
  status: number;
  body?: string;
  stream?: ReadableStream<Uint8Array>;
}

export function jsonResponse(status: number, body: unknown): RouteResult {
  return { status, body: JSON.stringify(body) };
}

export function mockFetch(handler: (url: string, init: RequestInit) => RouteResult | Promise<RouteResult>): typeof fetch {
  return (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const result = await handler(url, init ?? {});
    const stream = result.stream;
    return new Response(stream ?? result.body ?? null, { status: result.status });
  }) as typeof fetch;
}

export function negotiation(transports: TransportName[], extensions?: string[]) {
  return {
    protocolVersion: 1,
    connectionId: "negotiated-id",
    connectionToken: "ticket-1",
    expiresAt: "2026-01-01T00:00:00Z",
    availableTransports: transports.map((name) => ({
      name,
      transferFormats: ["Text"],
      extensions: name === "WebSockets" ? extensions : undefined,
    })),
  };
}

export function hello(id = "conn-1"): string {
  return JSON.stringify({ v: 1, type: "hello", id: "hello-1", origin: "server", payload: { connectionId: id } });
}

export function eventFrame(event: string, payload: unknown, id = "evt-1"): string {
  return JSON.stringify({ v: 1, type: "event", event, id, payload });
}
