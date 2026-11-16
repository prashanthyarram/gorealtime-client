import { RealtimeError } from "../errors.js";
import { toWebSocketURL } from "../negotiation.js";
import type { TransportArgs, TransportSession } from "./types.js";

/**
 * Browser WebSocket cannot set `Authorization` or the connection-token header.
 * The socket relies on cookies the browser attaches by itself. Ping and pong
 * are answered by the browser. permessage-deflate, when the server offers it
 * during the HTTP upgrade, is also handled by the browser. This function does
 * not compress payloads.
 */
export function connectWebSocket(args: TransportArgs): Promise<TransportSession> {
  const url = toWebSocketURL(args.httpURL);
  return new Promise((resolve, reject) => {
    let opened = false;
    let settled = false;
    let socket: WebSocket;
    try {
      socket = args.webSocketFactory(url);
    } catch (cause) {
      reject(new RealtimeError("transport_failed", "websocket constructor failed", { status: 0, cause }));
      return;
    }
    const fail = (error: RealtimeError) => {
      if (settled) {
        if (opened) {
          args.onDown(error);
        }
        return;
      }
      settled = true;
      cleanup();
      try {
        socket.close();
      } catch {
        /* already closed */
      }
      reject(error);
    };
    const onOpen = () => {
      if (settled) {
        return;
      }
      opened = true;
      settled = true;
      resolve({
        name: "WebSockets",
        send: async (body: string) => {
          if (socket.readyState !== WebSocket.OPEN) {
            throw new RealtimeError("send_failed", "websocket is not open");
          }
          socket.send(body);
        },
        close: () => {
          cleanup();
          socket.close();
        },
      });
    };
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== "string") {
        args.onDown(new RealtimeError("protocol", "websocket frame is not text"));
        return;
      }
      args.onText(event.data);
    };
    const onError = () => {
      fail(new RealtimeError("transport_failed", "websocket handshake failed", { status: 0 }));
    };
    const onClose = () => {
      cleanup();
      if (!opened) {
        fail(new RealtimeError("transport_failed", "websocket closed before open", { status: 0 }));
        return;
      }
      args.onDown(new RealtimeError("disconnected", "websocket closed"));
    };
    const cleanup = () => {
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("error", onError);
      socket.removeEventListener("close", onClose);
    };
    socket.addEventListener("open", onOpen);
    socket.addEventListener("message", onMessage);
    socket.addEventListener("error", onError);
    socket.addEventListener("close", onClose);
    if (args.signal.aborted) {
      fail(new RealtimeError("cancelled", "connection cancelled", { status: 0 }));
      return;
    }
    args.signal.addEventListener(
      "abort",
      () => {
        fail(new RealtimeError("cancelled", "connection cancelled", { status: 0 }));
      },
      { once: true },
    );
  });
}
