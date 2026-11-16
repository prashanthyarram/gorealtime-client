import assert from "node:assert/strict";
import { test } from "node:test";
import { RealtimeClient, RealtimeError } from "../dist/index.js";
import { eventFrame, hello, installSockets, jsonResponse, mockFetch, negotiation, type FakeSocket } from "./harness.ts";

const URL = "https://api.example.com/realtime";

function headerValue(init: RequestInit, name: string): string | null {
  const headers = new Headers(init.headers);
  return headers.get(name);
}

test("connect sends and receives on a mocked websocket", async () => {
  const sockets = installSockets();
  let sentAuth = "";
  const fetchImpl = mockFetch(async (url, init) => {
    assert.equal(url, `${URL}/negotiate`);
    sentAuth = headerValue(init, "authorization") ?? "";
    return jsonResponse(200, negotiation(["WebSockets", "ServerSentEvents", "LongPolling"]));
  });
  const client = new RealtimeClient({
    url: URL,
    fetch: fetchImpl,
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
    accessTokenFactory: async () => "app-token",
    attemptWebSocketWithBearer: true,
  });
  const states: string[] = [];
  client.onLifecycle("transport", (name) => states.push(String(name)));
  const received: unknown[] = [];
  client.on<{ text: string }>("chat.message", (message) => received.push(message.text));
  const connecting = client.connect();
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  assert.equal(socket.url, "wss://api.example.com/realtime");
  assert.equal(socket.url.includes("app-token"), false);
  socket.serverMessage(hello());
  await connecting;
  assert.equal(client.state, "connected");
  assert.equal(client.transport, "WebSockets");
  assert.equal(client.connectionId, "conn-1");
  assert.equal(sentAuth, "Bearer app-token");
  socket.serverMessage(eventFrame("chat.message", { text: "hi" }));
  assert.deepEqual(received, ["hi"]);
  await client.send("chat.send", { text: "yo" });
  const frame = JSON.parse(socket.sent[0] ?? "{}") as { type: string; event: string; payload: { text: string } };
  assert.equal(frame.type, "event");
  assert.equal(frame.event, "chat.send");
  assert.deepEqual(frame.payload, { text: "yo" });
  await client.disconnect();
  await client.disconnect();
  assert.equal(client.state, "disconnected");
  assert.equal(socket.closed, true);
});

test("a throwing listener does not block the next listener", async () => {
  const sockets = installSockets();
  const client = new RealtimeClient({
    url: URL,
    fetch: mockFetch(async () => jsonResponse(200, negotiation(["WebSockets"]))),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const errors: string[] = [];
  client.onLifecycle("error", (error) => errors.push((error as RealtimeError).code));
  client.on("chat.message", () => {
    throw new Error("app");
  });
  const seen: string[] = [];
  client.on("chat.message", () => seen.push("second"));
  const connecting = client.connect();
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  socket.serverMessage(hello());
  await connecting;
  socket.serverMessage(eventFrame("chat.message", { ok: true }));
  assert.deepEqual(seen, ["second"]);
  assert.deepEqual(errors, ["lifecycle"]);
  await client.disconnect();
});

test("off removes one handler and leaves the other", async () => {
  const sockets = installSockets();
  const client = new RealtimeClient({
    url: URL,
    fetch: mockFetch(async () => jsonResponse(200, negotiation(["WebSockets"]))),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const seen: string[] = [];
  const stop = client.on("chat.message", () => seen.push("a"));
  client.on("chat.message", () => seen.push("b"));
  const connecting = client.connect();
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  socket.serverMessage(hello());
  await connecting;
  stop();
  socket.serverMessage(eventFrame("chat.message", {}));
  assert.deepEqual(seen, ["b"]);
  client.off("chat.message");
  sockets.sockets[0].serverMessage(eventFrame("chat.message", {}, "evt-2"));
  assert.deepEqual(seen, ["b"]);
  await client.disconnect();
});

test("server error frames are reported and the socket stays up", async () => {
  const sockets = installSockets();
  const client = new RealtimeClient({
    url: URL,
    fetch: mockFetch(async () => jsonResponse(200, negotiation(["WebSockets"]))),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const codes: string[] = [];
  client.onLifecycle("error", (error) => codes.push((error as RealtimeError).code));
  const connecting = client.connect();
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  socket.serverMessage(hello());
  await connecting;
  socket.serverMessage(JSON.stringify({ v: 1, type: "error", error: { code: "handler_failed", message: "nope" } }));
  assert.equal(client.state, "connected");
  assert.deepEqual(codes, ["server"]);
  socket.serverMessage("{");
  assert.deepEqual(codes, ["server", "protocol"]);
  await client.disconnect();
});

test("send rejects when the client is disconnected and is not retried", async () => {
  const client = new RealtimeClient({ url: URL });
  await assert.rejects(client.send("chat.send", { text: "x" }), (error: unknown) => {
    assert.ok(error instanceof RealtimeError);
    assert.equal(error.code, "disconnected");
    return true;
  });
});

test("concurrent connect calls share one attempt", async () => {
  const sockets = installSockets();
  let negotiations = 0;
  const client = new RealtimeClient({
    url: URL,
    fetch: mockFetch(async () => {
      negotiations += 1;
      return jsonResponse(200, negotiation(["WebSockets"]));
    }),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const first = client.connect();
  const second = client.connect();
  assert.equal(first, second);
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  socket.serverMessage(hello());
  await first;
  assert.equal(negotiations, 1);
  assert.equal(sockets.sockets.length, 1);
  await client.disconnect();
});

test("authentication failure does not open a transport", async () => {
  const sockets = installSockets();
  const client = new RealtimeClient({
    url: URL,
    fetch: mockFetch(async () => ({ status: 401 })),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  await assert.rejects(client.connect(), (error: unknown) => error instanceof RealtimeError && error.code === "unauthorized");
  assert.equal(sockets.sockets.length, 0);
  assert.equal(client.state, "disconnected");
});

test("websocket failure falls back to SSE and a 401 does not", async () => {
  const sockets = installSockets();
  const sse = sseStream();
  let posts = 0;
  const fetchImpl = mockFetch(async (url, init) => {
    if (url.endsWith("/negotiate")) {
      return jsonResponse(200, negotiation(["WebSockets", "ServerSentEvents"]));
    }
    if (url.endsWith("/sse")) {
      assert.equal(headerValue(init, "x-gorealtime-connection-token"), "ticket-1");
      assert.equal(headerValue(init, "authorization"), "Bearer fresh");
      return { status: 200, stream: sse.stream };
    }
    posts += 1;
    return { status: 202 };
  });
  const client = new RealtimeClient({
    url: URL,
    fetch: fetchImpl,
    accessTokenFactory: async () => "fresh",
    attemptWebSocketWithBearer: true,
    webSocketFactory: (url) => {
      const socket = sockets.factory(url);
      queueMicrotask(() => socket.failHandshake());
      return socket as unknown as WebSocket;
    },
  });
  const fallbacks: string[] = [];
  client.onLifecycle("fallback", (info) => fallbacks.push(JSON.stringify(info)));
  const connecting = client.connect();
  sse.push(`data: ${hello("sse-id")}\n\n`);
  await connecting;
  assert.equal(client.transport, "ServerSentEvents");
  assert.equal(client.connectionId, "sse-id");
  assert.equal(fallbacks.length, 1);
  await client.send("chat.send", { text: "via-sse" });
  assert.equal(posts, 1);
  await client.disconnect();
});

test("authorization and protocol failures stop fallback", async () => {
  for (const status of [401, 403, 400]) {
    const sockets = installSockets();
    const client = new RealtimeClient({
      url: URL,
      transports: ["WebSockets", "ServerSentEvents", "LongPolling"],
      fetch: mockFetch(async (url) => {
        if (url.endsWith("/negotiate")) {
          return jsonResponse(200, negotiation(["WebSockets", "ServerSentEvents", "LongPolling"]));
        }
        return { status };
      }),
      webSocketFactory: (url) => {
        const socket = sockets.factory(url);
        queueMicrotask(() => socket.failHandshake());
        return socket as unknown as WebSocket;
      },
    });
    await assert.rejects(client.connect());
    assert.equal(client.state, "disconnected");
  }
});

test("a bearer token skips websocket unless the app opts in", async () => {
  const sockets = installSockets();
  const sse = sseStream();
  const client = new RealtimeClient({
    url: URL,
    accessTokenFactory: () => "tok",
    fetch: mockFetch(async (url) => {
      if (url.endsWith("/negotiate")) {
        return jsonResponse(200, negotiation(["WebSockets", "ServerSentEvents"]));
      }
      return { status: 200, stream: sse.stream };
    }),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const connecting = client.connect();
  sse.push(`data: ${hello()}\n\n`);
  await connecting;
  assert.equal(sockets.sockets.length, 0);
  assert.equal(client.transport, "ServerSentEvents");
  await client.disconnect();
});

test("disabled server transports are not attempted", async () => {
  const sockets = installSockets();
  let polls = 0;
  const client = new RealtimeClient({
    url: URL,
    fetch: mockFetch(async (url) => {
      if (url.endsWith("/negotiate")) {
        return jsonResponse(200, negotiation(["LongPolling"]));
      }
      polls += 1;
      if (polls === 1) {
        return jsonResponse(200, { messages: [JSON.parse(hello())] });
      }
      return { status: 204 };
    }),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  await client.connect();
  assert.equal(sockets.sockets.length, 0);
  assert.equal(client.transport, "LongPolling");
  await client.disconnect();
});

test("long poll reads a batch, treats 204 as empty, and posts events", async () => {
  let polls = 0;
  let posted = "";
  const client = new RealtimeClient({
    url: URL,
    transports: ["LongPolling"],
    fetch: mockFetch(async (url, init) => {
      if (url.endsWith("/negotiate")) {
        return jsonResponse(200, negotiation(["LongPolling"]));
      }
      if (url.endsWith("/send")) {
        posted = String(init.body);
        return { status: 202 };
      }
      polls += 1;
      if (polls === 1) {
        return jsonResponse(200, {
          messages: [JSON.parse(hello("poll-id")), JSON.parse(eventFrame("echo", { n: 2 }))],
        });
      }
      return { status: 204 };
    }),
  });
  const values: number[] = [];
  client.on<{ n: number }>("echo", (payload) => values.push(payload.n));
  await client.connect();
  assert.equal(client.connectionId, "poll-id");
  assert.deepEqual(values, [2]);
  await client.send("echo", { n: 3 });
  assert.equal(JSON.parse(posted).event, "echo");
  await client.disconnect();
});

test("unexpected disconnect retries with a refreshed token and does not resend", async () => {
  const sockets = installSockets();
  let tokens: string[] = [];
  let generation = 0;
  const client = new RealtimeClient({
    url: URL,
    automaticReconnect: true,
    reconnect: { initialDelayMs: 1, maxDelayMs: 5, jitter: 0, maxRetries: 2 },
    random: () => 0,
    accessTokenFactory: async () => `tok-${tokens.length}`,
    attemptWebSocketWithBearer: true,
    fetch: mockFetch(async (url, init) => {
      if (url.endsWith("/negotiate")) {
        tokens.push(headerValue(init, "authorization") ?? "");
        generation += 1;
        return jsonResponse(200, negotiation(["WebSockets"]));
      }
      return { status: 500 };
    }),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const lifecycle: string[] = [];
  client.onLifecycle("reconnecting", () => lifecycle.push("reconnecting"));
  client.onLifecycle("reconnected", () => lifecycle.push("reconnected"));
  const connecting = client.connect();
  const first = await nextSocket(sockets.sockets);
  first.open();
  first.serverMessage(hello());
  await connecting;
  await client.send("chat.send", { text: "once" });
  const sentBefore = first.sent.length;
  first.close();
  const second = await nextSocket(sockets.sockets, 2);
  second.open();
  second.serverMessage(hello("conn-2"));
  await waitFor(() => lifecycle.includes("reconnected"));
  assert.equal(lifecycle[0], "reconnecting");
  assert.equal(client.state, "connected");
  assert.equal(tokens[0], "Bearer tok-0");
  assert.ok(tokens.length >= 2);
  assert.notEqual(tokens[1], tokens[0]);
  assert.equal(second.sent.length, 0);
  assert.equal(sentBefore, 1);
  assert.ok(generation >= 2);
  await client.disconnect();
});

test("manual disconnect during reconnect cancels the loop", async () => {
  const sockets = installSockets();
  const client = new RealtimeClient({
    url: URL,
    automaticReconnect: true,
    reconnect: { initialDelayMs: 60_000, maxDelayMs: 60_000, jitter: 0, maxRetries: 5 },
    fetch: mockFetch(async () => jsonResponse(200, negotiation(["WebSockets"]))),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const connecting = client.connect();
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  socket.serverMessage(hello());
  await connecting;
  socket.close();
  await waitFor(() => client.state === "reconnecting");
  await client.disconnect();
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(client.state, "disconnected");
  assert.equal(sockets.sockets.length, 1);
});

test("reconnect stops on an authentication failure", async () => {
  const sockets = installSockets();
  let calls = 0;
  const client = new RealtimeClient({
    url: URL,
    automaticReconnect: true,
    reconnect: { initialDelayMs: 1, maxDelayMs: 5, jitter: 0, maxRetries: 4 },
    fetch: mockFetch(async () => {
      calls += 1;
      if (calls === 1) {
        return jsonResponse(200, negotiation(["WebSockets"]));
      }
      return { status: 401 };
    }),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const errors: string[] = [];
  client.onLifecycle("error", (error) => errors.push((error as RealtimeError).code));
  const connecting = client.connect();
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  socket.serverMessage(hello());
  await connecting;
  socket.close();
  await waitFor(() => errors.includes("unauthorized"));
  assert.equal(client.state, "disconnected");
  assert.equal(calls, 2);
});

test("every transport failing ends the attempt", async () => {
  let opens = 0;
  const client = new RealtimeClient({
    url: URL,
    fetch: mockFetch(async (url) => {
      if (url.endsWith("/negotiate")) {
        return jsonResponse(200, negotiation(["WebSockets", "ServerSentEvents", "LongPolling"]));
      }
      opens += 1;
      return { status: 502 };
    }),
    webSocketFactory: (url) => {
      const socket = installSockets().factory(url);
      queueMicrotask(() => socket.failHandshake());
      return socket as unknown as WebSocket;
    },
  });
  await assert.rejects(client.connect(), (error: unknown) => error instanceof RealtimeError && error.code === "transport_failed");
  assert.equal(opens, 2);
  assert.equal(client.state, "disconnected");
});

test("reconnect stops after the configured number of attempts", async () => {
  const sockets = installSockets();
  let negotiations = 0;
  const client = new RealtimeClient({
    url: URL,
    automaticReconnect: true,
    reconnect: { initialDelayMs: 1, maxDelayMs: 5, jitter: 0, maxRetries: 2 },
    fetch: mockFetch(async () => {
      negotiations += 1;
      if (negotiations === 1) {
        return jsonResponse(200, negotiation(["WebSockets"]));
      }
      return { status: 502 };
    }),
    webSocketFactory: (url) => sockets.factory(url) as unknown as WebSocket,
  });
  const errors: string[] = [];
  client.onLifecycle("error", (error) => errors.push((error as RealtimeError).code));
  const connecting = client.connect();
  const socket = await nextSocket(sockets.sockets);
  socket.open();
  socket.serverMessage(hello());
  await connecting;
  socket.close();
  await waitFor(() => errors.includes("reconnect_failed"));
  assert.equal(client.state, "disconnected");
  assert.equal(negotiations, 3);
});

function sseStream() {
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(ctrl) {
      controller = ctrl;
    },
  });
  return {
    stream,
    push(text: string) {
      controller?.enqueue(encoder.encode(text));
    },
  };
}

function nextSocket(sockets: FakeSocket[], count = 1): Promise<FakeSocket> {
  return waitForSocket(sockets, count);
}

function waitForSocket(sockets: FakeSocket[], count: number): Promise<FakeSocket> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (sockets.length >= count) {
        clearInterval(timer);
        resolve(sockets[count - 1]);
      } else if (Date.now() - started > 2000) {
        clearInterval(timer);
        reject(new Error("socket was not created"));
      }
    }, 5);
  });
}

function waitFor(check: () => boolean): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (check()) {
        clearInterval(timer);
        resolve();
      } else if (Date.now() - started > 2000) {
        clearInterval(timer);
        reject(new Error("timed out"));
      }
    }, 5);
  });
}
