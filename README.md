# @gorealtime/client

[![npm](https://img.shields.io/npm/v/@gorealtime/client)](https://www.npmjs.com/package/@gorealtime/client)

The npm package is [@gorealtime/client](https://www.npmjs.com/package/@gorealtime/client).

`@gorealtime/client` is the browser client for [gorealtime](https://github.com/prashanthyarram/gorealtime), a process-local realtime library for Go. This repository is only the client. The server stays in the Go repository.

The server accepts a connection, gives it an id, and delivers JSON events until the peer leaves or the process shuts down. This package opens that connection, picks a transport, and delivers named events to your handlers. It does not store messages, replay them, manage users, or implement chat.

This is not the SignalR protocol. The envelope, paths, and fallback rules are the ones implemented by the Go server.

## Installation

```bash
npm install @gorealtime/client
```

The package is ESM. It has no runtime dependencies. TypeScript declarations are included.

## Quick start

```typescript
import { RealtimeClient } from "@gorealtime/client";

const client = new RealtimeClient({
  url: "https://api.example.com/realtime",
  automaticReconnect: true,
});

client.on("chat.message", (payload) => {
  console.log(payload);
});

await client.connect();
await client.send("chat.send", { text: "Hello from the frontend!" });
await client.disconnect();
```

`url` is the HTTP mount path, the same path the Go server registers. The client turns it into `wss://` for WebSocket.

## Configuration

| Option | Meaning |
| --- | --- |
| `url` | HTTP or HTTPS URL of the mount path. Required. |
| `accessTokenFactory` | Async function that returns the application access token. Sent as `Authorization: Bearer <token>` on HTTP requests. A value that already starts with `Bearer ` is sent unchanged. Not written into a URL. |
| `attemptWebSocketWithBearer` | When a token factory is set, WebSocket is skipped unless this is `true`. The browser still cannot attach the bearer token to the upgrade. Use it when cookies authenticate the socket. |
| `credentials` | Fetch credentials mode. Default `same-origin`, so cookies go to the same site. |
| `headers` | Extra headers for negotiate, SSE, long polling, and POST. Either an object or an async function. Ignored by the browser WebSocket handshake. |
| `transports` | Preference order. Default `WebSockets`, `ServerSentEvents`, `LongPolling`. A transport the server did not offer is not attempted. |
| `automaticReconnect` | Start a new connection after an unexpected drop. Default `false`. |
| `reconnect` | `initialDelayMs` (1000), `maxDelayMs` (30000), `jitter` (0.2), `maxRetries` (5). |
| `maxPayloadBytes` | Largest JSON payload this client will send. Default 32 KiB, matching the server. |
| `helloTimeoutMs` | How long to wait for the server `hello` frame. Default 10 seconds. |

## Authentication

The client does not collect passwords or issue credentials. Your application does.

```typescript
const client = new RealtimeClient({
  url: "https://api.example.com/realtime",
  accessTokenFactory: async () => myAuth.getAccessToken(),
  credentials: "include",
});
```

The factory is called again for each negotiate, SSE request, long poll, and POST, so a refresh inside `getAccessToken` is picked up on the next request.

Cookie sessions use `credentials`. The browser attaches those cookies to `fetch`. A same-site cookie is also attached to the WebSocket upgrade by the browser. The client never puts a bearer token or the connection token in the URL.

The native WebSocket API cannot set `Authorization` or `X-Gorealtime-Connection-Token`. When `accessTokenFactory` is set, the client therefore uses SSE or long polling, both of which can send those headers. Set `attemptWebSocketWithBearer` only when the upgrade is authenticated some other way, usually a cookie.

## Events

```typescript
interface ChatMessage {
  text: string;
  sender: string;
}

const stop = client.on<ChatMessage>("chat.message", (message, envelope) => {
  console.log(message.text, envelope.id);
});

stop();
client.off("chat.message", handler);
client.off("chat.message");
```

Handlers for one event run in registration order. If one throws, the others still run, and the error is reported on the `error` lifecycle event. The client does not drop the connection because an application handler threw.

`off` without a handler removes every handler for that event. The function returned by `on` removes only the handler it registered.

## Sending

```typescript
await client.send("chat.send", { text: "hi" });
await client.ack(envelope.id ?? "");
```

`send` and `ack` reject when the client is not connected or the write fails. A rejected send is not retried and is not sent again after reconnect. The server does not persist the event.

An `ack` means this client claims to have seen that frame id. The server treats it the same way: it is not proof that an application handler ran, and it is not a durable receipt.

## Connection state

`client.state` is `disconnected`, `connecting`, `connected`, `reconnecting`, or `disconnecting`.

```typescript
client.onLifecycle("connected", () => {});
client.onLifecycle("disconnected", () => {});
client.onLifecycle("reconnecting", () => {});
client.onLifecycle("reconnected", () => {});
client.onLifecycle("error", (error) => {});
client.onLifecycle("transport", (name) => {});
client.onLifecycle("fallback", (info) => {});
```

Lifecycle listeners run synchronously, in registration order. `connected` runs every time the state becomes connected. `reconnected` runs only after a reconnect loop has opened a new connection. `error` receives a `RealtimeError`. `transport` receives the selected transport name. `fallback` receives `{ from, to, status }`. `to` is `null` when fallback stops.

`connect()` rejects on failure. A second `connect()` while one attempt is in progress returns the same promise. `disconnect()` is safe to call more than once and cancels a reconnect wait.

`client.transport` is the transport that is up now. `client.connectionId` is the id from the server `hello` frame.

## Transports and fallback

The client always calls `POST <url>/negotiate` first. The response lists the transports this process will accept. The client walks its preference list and keeps only those names.

Default order:

1. WebSocket (`GET <url>`, upgraded)
2. Server-sent events (`GET <url>/sse`)
3. Long polling (`GET <url>/poll`)

SSE and long polling send `X-Gorealtime-Connection-Token` from the negotiate response. Client events on those transports are `POST <url>/send` and expect HTTP 202. A long poll that returns 204 is an empty wait, not an error. The client keeps one poll in flight. A 409 means the previous poll has not finished; the client retries that poll a few times instead of opening a second one.

Fallback follows the server's `ShouldFallback` rule. The client tries the next transport only for status 0, 404, 501, 502, and 504. It stops on 400, 401, 403, 409, 413, 429, and 503. Status 0 means the request did not produce an HTTP status.

A browser WebSocket handshake failure does not expose the HTTP status, so the client classifies it as status 0 and may try the next transport. A 401 from the upgrade is therefore indistinguishable from a dead proxy. The following SSE or long-poll attempt then returns 401 and fallback stops. Applications that use a bearer token skip WebSocket for this reason.

## Reconnection

Set `automaticReconnect: true` to negotiate again after an unexpected close. The delay is exponential, capped, and jittered. `maxRetries` limits the attempts after the drop. There is one reconnect loop. `disconnect()` cancels it.

Each attempt calls `accessTokenFactory` again. Messages that failed, or that were never sent, are not written on the new connection. The server does not store or replay events from the old one. If the application needs those messages, it has to keep them itself.

A reconnect stops early on authentication, authorization, and other non-retryable protocol errors.

## Compression

WebSocket compression is `permessage-deflate`, negotiated by the browser during the HTTP upgrade. The client does not compress JSON itself. The extension is used only when the Go server was started with compression enabled and the browser offers parameters the server accepts. Configuring nothing on the client does not mean compression is active.

SSE and long polling use ordinary HTTP compression. The browser sends `Accept-Encoding` and decodes `Content-Encoding`. The Go server compresses those responses when the request asks for gzip. That is not `permessage-deflate`.

## Errors

`RealtimeError` has a `code`, a short `message`, an optional HTTP `status`, and `cause` when there was an underlying error. Messages do not include tokens or header values.

| Code | When |
| --- | --- |
| `negotiation_failed` | The negotiate request could not be completed. |
| `unauthorized` | HTTP 401. |
| `forbidden` | HTTP 403. |
| `transport_failed` | The selected transport did not open. |
| `protocol` | A frame or negotiate body broke the version 1 rules. |
| `serialization` | This client refused a frame it was asked to send. |
| `server` | The server sent an `error` frame. The connection stays up. |
| `send_failed` | A write or POST failed. |
| `disconnected` | The socket closed, or `send` was called while down. |
| `cancelled` | `disconnect()` won the race with `connect()`. |
| `reconnect_failed` | The retry budget ran out. |
| `lifecycle` | An application listener threw. |

`connect`, `send`, and `ack` reject. Transport loss and handler exceptions are reported through `onLifecycle("error")` as well. A server `error` frame does not reject `connect` and does not close the connection. A `bye` frame does.

## Browser support

The first release targets current versions of Chrome, Firefox, Safari, and Edge: a native `WebSocket`, `fetch`, `AbortController`, `ReadableStream`, and `TextDecoder`. Those have been in the evergreen browsers since 2022. The package does not use Node.js APIs in its published code.

If `fetch` cannot stream a body, SSE fails to open and the client falls through to long polling when the server offers it and the failure is retryable.

Node.js 22 can run the tests because it provides the same globals. Node is not a supported application runtime for this release.

The client does not listen for `pagehide` or `beforeunload`. A tab closing the page drops the connection the way the browser always does. The client does not disconnect early.

## Known limitations

- The server is process-local. A dropped connection loses every event that was not already delivered. Reconnect does not recover them.
- The browser cannot send the connection token on WebSocket. That handshake creates a new connection id. The negotiated ticket is left unused and expires (the server default is 30 seconds). SSE and long polling send the token and keep the negotiated id.
- A WebSocket handshake failure has no HTTP status in the browser, so it is treated as a fallback-eligible failure.
- The client does not accept the connection token from the query string. The server does not either.
- The client speaks JSON only. The server does not offer another encoding.
- There is no message buffer, presence list, or group API on the client. Groups are a server-side concern.

## Examples

- `examples/plain.js`
- `examples/typescript.ts`
- `examples/browser.html`
- `examples/react/App.tsx` uses React only in the example. React is not a dependency of this package.
- `examples/vue/ChatPanel.ts` uses Vue only in the example. Vue is not a dependency of this package.

## Development

```bash
npm install
npm run build
npm test
npm run typecheck
npm run size
npm pack --dry-run
```

`npm test` runs the unit tests and, when Go is available, connects this client to `testserver`. That program is a small echo server. It is the only part of this repository that imports the Go module, pinned to commit `808eba9` on `prashanth/transports`. Tooling dependencies are `typescript` and `esbuild`. Neither is loaded by an application that imports the package.

## License

MIT. Copyright (c) 2026 Prashanth Yarram.
