import type { RealtimeError } from "../errors.js";
import type { TransportName } from "../negotiation.js";

export interface TransportSession {
  readonly name: TransportName;
  send(body: string): Promise<void>;
  close(): void;
}

export interface TransportArgs {
  httpURL: string;
  token: string;
  credentials: RequestCredentials;
  signal: AbortSignal;
  fetchImpl: typeof fetch;
  webSocketFactory: (url: string) => WebSocket;
  headers: () => Promise<Headers>;
  onText: (text: string) => void;
  onDown: (error: RealtimeError) => void;
}
