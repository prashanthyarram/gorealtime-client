import assert from "node:assert/strict";
import { test } from "node:test";
import { retryDelay } from "../dist/index.js";

test("retryDelay grows exponentially and stays inside the jitter band", () => {
  const options = { initialDelayMs: 1000, maxDelayMs: 30_000, jitter: 0.2 };
  assert.equal(retryDelay(0, { ...options, jitter: 0 }, () => 0), 1000);
  assert.equal(retryDelay(1, { ...options, jitter: 0 }, () => 0), 2000);
  assert.equal(retryDelay(2, { ...options, jitter: 0 }, () => 0), 4000);
  assert.equal(retryDelay(0, options, () => 0), 800);
  assert.equal(retryDelay(0, options, () => 1), 1200);
  assert.equal(retryDelay(20, { ...options, jitter: 0 }, () => 0), 30_000);
});
