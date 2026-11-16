import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { RealtimeClient, RealtimeError, type TransportName } from "../dist/index.js";

const repoRoot = fileURLToPath(new URL("..", import.meta.url));

test("the client exchanges events with the Go server on every transport", { timeout: 90_000 }, async () => {
  const server = await startServer();
  try {
    await roundTrip(server.url, ["WebSockets"]);
    await roundTrip(server.url, ["ServerSentEvents"]);
    await roundTrip(server.url, ["LongPolling"]);
  } finally {
    server.stop();
  }
});

test("websocket is skipped when the server does not offer it", { timeout: 90_000 }, async () => {
  const server = await startServer({ TRANSPORTS: "ServerSentEvents,LongPolling" });
  try {
    const fallbacks: string[] = [];
    const client = new RealtimeClient({ url: server.url });
    client.onLifecycle("fallback", () => fallbacks.push("fallback"));
    await client.connect();
    assert.equal(client.transport, "ServerSentEvents");
    assert.equal(fallbacks.length, 0);
    await echo(client, "sse-only");
    await client.disconnect();
  } finally {
    server.stop();
  }
});

test("a bearer token is sent on HTTP and a missing token is unauthorized", { timeout: 90_000 }, async () => {
  const server = await startServer({ REQUIRE_BEARER: "secret" });
  try {
    const denied = new RealtimeClient({ url: server.url, transports: ["ServerSentEvents"] });
    await assert.rejects(denied.connect(), (error: unknown) => error instanceof RealtimeError && error.code === "unauthorized");

    const client = new RealtimeClient({
      url: server.url,
      transports: ["ServerSentEvents"],
      accessTokenFactory: async () => "secret",
    });
    await client.connect();
    assert.equal(client.transport, "ServerSentEvents");
    await echo(client, "authed");
    await client.disconnect();
  } finally {
    server.stop();
  }
});

async function roundTrip(url: string, transports: TransportName[]): Promise<void> {
  const client = new RealtimeClient({ url, transports });
  await client.connect();
  assert.equal(client.transport, transports[0]);
  assert.ok(client.connectionId);
  await echo(client, transports[0]);
  await client.disconnect();
  assert.equal(client.state, "disconnected");
}

async function echo(client: RealtimeClient, text: string): Promise<void> {
  const received = new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no echo for ${text}`)), 10_000);
    client.on<{ text: string }>("echo", (payload) => {
      if (payload.text === text) {
        clearTimeout(timer);
        resolve(payload.text);
      }
    });
  });
  await client.send("echo", { text });
  assert.equal(await received, text);
}

function startServer(env: Record<string, string> = {}): Promise<{ url: string; stop: () => void }> {
  return new Promise((resolve, reject) => {
    const child: ChildProcess = spawn("go", ["run", "./testserver"], {
      cwd: repoRoot,
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
      detached: true,
    });
    let buffer = "";
    const kill = () => {
      if (!child.pid) {
        return;
      }
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {
        child.kill("SIGKILL");
      }
    };
    const fail = (error: Error) => {
      kill();
      reject(error);
    };
    child.stdout?.on("data", (chunk: Buffer) => {
      buffer += chunk.toString();
      const match = buffer.match(/LISTEN (http:\/\/\S+)/);
      if (match) {
        resolve({
          url: match[1],
          stop: kill,
        });
      }
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      const text = chunk.toString();
      if (text.includes("panic")) {
        fail(new Error(text));
      }
    });
    child.on("exit", (code) => {
      if (!buffer.includes("LISTEN")) {
        fail(new Error(`test server exited ${code}: ${buffer}`));
      }
    });
  });
}
