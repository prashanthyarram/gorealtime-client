import assert from "node:assert/strict";
import { test } from "node:test";
import { parseNegotiation, selectTransports, shouldFallback } from "../dist/index.js";

test("parseNegotiation reads the Go response shape", () => {
  const parsed = parseNegotiation({
    protocolVersion: 1,
    connectionId: "abc",
    connectionToken: "tok",
    expiresAt: "2026-01-01T00:00:00Z",
    availableTransports: [
      { name: "WebSockets", transferFormats: ["Text"], extensions: ["permessage-deflate"] },
      { name: "ServerSentEvents", transferFormats: ["Text"] },
      { name: "LongPolling", transferFormats: ["Text"] },
    ],
  });
  assert.equal(parsed.connectionToken, "tok");
  assert.deepEqual(parsed.availableTransports[0].extensions, ["permessage-deflate"]);
  assert.equal(parsed.availableTransports[1].extensions, undefined);
});

test("parseNegotiation rejects a different protocol version", () => {
  assert.throws(() =>
    parseNegotiation({
      protocolVersion: 2,
      connectionId: "abc",
      connectionToken: "tok",
      expiresAt: "2026-01-01T00:00:00Z",
      availableTransports: [],
    }),
  );
});

test("selectTransports keeps client preference and drops transports the server did not offer", () => {
  const selected = selectTransports(["WebSockets", "ServerSentEvents", "LongPolling"], [
    { name: "LongPolling", transferFormats: ["Text"] },
    { name: "ServerSentEvents", transferFormats: ["Text"] },
  ]);
  assert.deepEqual(selected, ["ServerSentEvents", "LongPolling"]);
});

test("shouldFallback matches the Go status list", () => {
  for (const status of [0, 404, 501, 502, 504]) {
    assert.equal(shouldFallback(status), true, String(status));
  }
  for (const status of [400, 401, 403, 409, 413, 429, 503, 200]) {
    assert.equal(shouldFallback(status), false, String(status));
  }
});
