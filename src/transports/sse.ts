import { RealtimeError, httpFailure } from "../errors.js";
import { CONNECTION_TOKEN_HEADER, join } from "../negotiation.js";
import { createSSEParser } from "../sse.js";
import type { TransportArgs, TransportSession } from "./types.js";

export async function connectSSE(args: TransportArgs): Promise<TransportSession> {
  const headers = await args.headers();
  headers.set(CONNECTION_TOKEN_HEADER, args.token);
  let response: Response;
  try {
    response = await args.fetchImpl(join(args.httpURL, "sse"), {
      method: "GET",
      headers,
      credentials: args.credentials,
      signal: args.signal,
    });
  } catch (cause) {
    throw new RealtimeError("transport_failed", "sse request failed", { status: 0, cause });
  }
  if (!response.ok) {
    throw httpFailure(response.status, "sse");
  }
  if (!response.body) {
    throw new RealtimeError("transport_failed", "sse response has no body", { status: response.status });
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const parser = createSSEParser((data) => args.onText(data));
  let stopped = false;
  const stop = () => {
    if (stopped) {
      return;
    }
    stopped = true;
    reader.cancel().catch(() => undefined);
  };
  const pump = async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        parser.push(decoder.decode(value, { stream: true }));
      }
      if (!stopped) {
        args.onDown(new RealtimeError("disconnected", "sse stream ended"));
      }
    } catch (cause) {
      if (!stopped && !args.signal.aborted) {
        args.onDown(new RealtimeError("disconnected", "sse stream failed", { cause }));
      }
    }
  };
  void pump();
  args.signal.addEventListener("abort", stop, { once: true });
  return {
    name: "ServerSentEvents",
    send: (body) => postSend(args, body),
    close: stop,
  };
}

export async function postSend(args: TransportArgs, body: string): Promise<void> {
  const headers = await args.headers();
  headers.set(CONNECTION_TOKEN_HEADER, args.token);
  headers.set("Content-Type", "application/json");
  let response: Response;
  try {
    response = await args.fetchImpl(join(args.httpURL, "send"), {
      method: "POST",
      headers,
      body,
      credentials: args.credentials,
      signal: args.signal,
    });
  } catch (cause) {
    throw new RealtimeError("send_failed", "send request failed", { status: 0, cause });
  }
  if (response.status !== 202) {
    const err = httpFailure(response.status, "send");
    throw new RealtimeError("send_failed", err.message, { status: response.status, cause: err });
  }
}
