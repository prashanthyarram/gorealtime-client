import { RealtimeError, httpFailure } from "../errors.js";
import { CONNECTION_TOKEN_HEADER, join } from "../negotiation.js";
import { postSend } from "./sse.js";
import type { TransportArgs, TransportSession } from "./types.js";

export async function connectLongPolling(args: TransportArgs): Promise<TransportSession> {
  let stopped = false;
  const stop = () => {
    stopped = true;
  };
  args.signal.addEventListener("abort", stop, { once: true });
  await pollOnce(args, true);
  const loop = async () => {
    let conflicts = 0;
    while (!stopped && !args.signal.aborted) {
      try {
        const again = await pollOnce(args, false);
        if (again === "conflict") {
          conflicts += 1;
          if (conflicts > 5) {
            args.onDown(httpFailure(409, "long poll"));
            return;
          }
          await wait(20, args.signal);
          continue;
        }
        conflicts = 0;
      } catch (cause) {
        if (stopped || args.signal.aborted) {
          return;
        }
        const error = cause instanceof RealtimeError ? cause : new RealtimeError("disconnected", "long poll failed", { cause });
        args.onDown(error);
        return;
      }
    }
  };
  void loop();
  return {
    name: "LongPolling",
    send: (body) => postSend(args, body),
    close: stop,
  };
}

async function pollOnce(args: TransportArgs, establishing: boolean): Promise<"conflict" | "ok"> {
  const headers = await args.headers();
  headers.set(CONNECTION_TOKEN_HEADER, args.token);
  let response: Response;
  try {
    response = await args.fetchImpl(join(args.httpURL, "poll"), {
      method: "GET",
      headers,
      credentials: args.credentials,
      signal: args.signal,
    });
  } catch (cause) {
    throw new RealtimeError(establishing ? "transport_failed" : "disconnected", "long poll request failed", {
      status: 0,
      cause,
    });
  }
  if (response.status === 204) {
    return "ok";
  }
  if (response.status === 409 && !establishing) {
    return "conflict";
  }
  if (!response.ok) {
    throw httpFailure(response.status, "long poll");
  }
  let body: unknown;
  try {
    body = await response.json();
  } catch (cause) {
    throw new RealtimeError("protocol", "long poll body is not JSON", { cause });
  }
  if (body === null || typeof body !== "object" || !Array.isArray((body as { messages?: unknown }).messages)) {
    throw new RealtimeError("protocol", "long poll body has no messages");
  }
  for (const message of (body as { messages: unknown[] }).messages) {
    args.onText(JSON.stringify(message));
  }
  return "ok";
}

function wait(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const id = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(id);
        resolve();
      },
      { once: true },
    );
  });
}
