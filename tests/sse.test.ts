import assert from "node:assert/strict";
import { test } from "node:test";
import { createSSEParser } from "../dist/sse.js";

test("parser splits several events in one chunk and ignores a comment", () => {
  const events: string[] = [];
  const comments: string[] = [];
  const parser = createSSEParser(
    (data) => events.push(data),
    (text) => comments.push(text),
  );
  parser.push(': ping\n\ndata: {"v":1}\n\ndata: {"v":1,"type":"event"}\n\n');
  assert.deepEqual(comments, ["ping"]);
  assert.deepEqual(events, ['{"v":1}', '{"v":1,"type":"event"}']);
});

test("parser waits for the rest of a partial event", () => {
  const events: string[] = [];
  const parser = createSSEParser((data) => events.push(data));
  parser.push("data: {\"a\":");
  assert.deepEqual(events, []);
  parser.push("1}\n\n");
  assert.deepEqual(events, ['{"a":1}']);
});

test("parser joins a multi-line data field", () => {
  const events: string[] = [];
  const parser = createSSEParser((data) => events.push(data));
  parser.push("data: one\ndata: two\n\n");
  assert.deepEqual(events, ["one\ntwo"]);
});
