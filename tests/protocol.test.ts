import assert from "node:assert/strict";
import { test } from "node:test";
import { RealtimeError, decodeServer, encodeClient, validateEventName } from "../dist/index.js";

test("encodeClient writes a version 1 event and keeps the id", () => {
  const text = encodeClient({ v: 1, type: "event", event: "chat.send", id: "abc", payload: { text: "hi" } }, 1024);
  const parsed = JSON.parse(text) as { v: number; type: string; event: string; id: string; payload: { text: string } };
  assert.equal(parsed.v, 1);
  assert.equal(parsed.type, "event");
  assert.equal(parsed.event, "chat.send");
  assert.equal(parsed.id, "abc");
  assert.deepEqual(parsed.payload, { text: "hi" });
});

test("encodeClient rejects a client hello and an oversized payload", () => {
  assert.throws(() => encodeClient({ v: 1, type: "hello" }, 1024), (error: unknown) => {
    assert.ok(error instanceof RealtimeError);
    assert.equal(error.code, "serialization");
    return true;
  });
  assert.throws(() => encodeClient({ v: 1, type: "event", event: "chat.send", payload: { blob: "x".repeat(80) } }, 16), (error: unknown) => {
    assert.ok(error instanceof RealtimeError);
    assert.equal(error.code, "serialization");
    return true;
  });
});

test("encodeClient requires a correlation id on ack", () => {
  assert.throws(() => encodeClient({ v: 1, type: "ack", id: "a1" }, 1024));
  const text = encodeClient({ v: 1, type: "ack", id: "a1", corr: "seen-1" }, 1024);
  assert.equal(JSON.parse(text).corr, "seen-1");
});

test("validateEventName accepts the server token alphabet", () => {
  validateEventName("chat.message");
  validateEventName("a_b:c-d.e");
  assert.throws(() => validateEventName("has space"));
  assert.throws(() => validateEventName(""));
});

test("decodeServer accepts hello, event, and error frames", () => {
  const hello = decodeServer(JSON.stringify({ v: 1, type: "hello", id: "h", payload: { connectionId: "c1" } }));
  assert.equal(hello.type, "hello");
  const event = decodeServer(JSON.stringify({ v: 1, type: "event", event: "echo", id: "e1", payload: { n: 1 } }));
  assert.equal(event.event, "echo");
  assert.deepEqual(event.payload, { n: 1 });
  const error = decodeServer(JSON.stringify({ v: 1, type: "error", error: { code: "handler_failed", message: "nope" } }));
  assert.equal(error.error?.code, "handler_failed");
});

test("decodeServer rejects a bad version, bad JSON, and a hello without connectionId", () => {
  assert.throws(() => decodeServer("{"), (error: unknown) => error instanceof RealtimeError && error.code === "protocol");
  assert.throws(() => decodeServer(JSON.stringify({ v: 2, type: "event", event: "a" })));
  assert.throws(() => decodeServer(JSON.stringify({ v: 1, type: "hello", payload: {} })));
  assert.throws(() => decodeServer(JSON.stringify({ v: 1, type: "event" })));
});
