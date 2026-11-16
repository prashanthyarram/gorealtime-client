import { RealtimeError, httpFailure } from "./errors.js";
import { PROTOCOL_VERSION } from "./protocol.js";

/** Transport names as the Go server spells them. */
export const TRANSPORTS = ["WebSockets", "ServerSentEvents", "LongPolling"] as const;

export type TransportName = (typeof TRANSPORTS)[number];

export interface TransportDescription {
  name: string;
  transferFormats: string[];
  /** WebSocket extensions only. Absent for SSE and long polling. */
  extensions?: string[];
}

export interface Negotiation {
  protocolVersion: number;
  connectionId: string;
  connectionToken: string;
  expiresAt: string;
  availableTransports: TransportDescription[];
}

export const CONNECTION_TOKEN_HEADER = "X-Gorealtime-Connection-Token";

/**
 * Same rule as Go `ShouldFallback`. Status 0 is a failed dial.
 * 401, 403, 400, 409, 413, 429, and 503 do not fall back.
 */
export function shouldFallback(status: number): boolean {
  return status === 0 || status === 404 || status === 501 || status === 502 || status === 504;
}

export function parseNegotiation(value: unknown): Negotiation {
  if (value === null || typeof value !== "object") {
    throw new RealtimeError("protocol", "negotiation body is not an object");
  }
  const raw = value as Record<string, unknown>;
  if (raw.protocolVersion !== PROTOCOL_VERSION) {
    throw new RealtimeError("protocol", "negotiation protocol version is not supported");
  }
  if (typeof raw.connectionId !== "string" || raw.connectionId.length === 0) {
    throw new RealtimeError("protocol", "negotiation is missing connectionId");
  }
  if (typeof raw.connectionToken !== "string" || raw.connectionToken.length === 0) {
    throw new RealtimeError("protocol", "negotiation is missing connectionToken");
  }
  if (typeof raw.expiresAt !== "string") {
    throw new RealtimeError("protocol", "negotiation is missing expiresAt");
  }
  if (!Array.isArray(raw.availableTransports)) {
    throw new RealtimeError("protocol", "negotiation is missing transports");
  }
  const availableTransports: TransportDescription[] = [];
  for (const item of raw.availableTransports) {
    if (item === null || typeof item !== "object") {
      throw new RealtimeError("protocol", "negotiation transport is invalid");
    }
    const row = item as Record<string, unknown>;
    if (typeof row.name !== "string") {
      throw new RealtimeError("protocol", "negotiation transport name is invalid");
    }
    const formats = Array.isArray(row.transferFormats)
      ? row.transferFormats.filter((f): f is string => typeof f === "string")
      : [];
    const extensions = Array.isArray(row.extensions)
      ? row.extensions.filter((f): f is string => typeof f === "string")
      : undefined;
    availableTransports.push({ name: row.name, transferFormats: formats, extensions });
  }
  return {
    protocolVersion: PROTOCOL_VERSION,
    connectionId: raw.connectionId,
    connectionToken: raw.connectionToken,
    expiresAt: raw.expiresAt,
    availableTransports,
  };
}

/** Client preference order, keeping only transports the server offered. */
export function selectTransports(preference: readonly TransportName[], offered: readonly TransportDescription[]): TransportName[] {
  const names = new Set(offered.map((t) => t.name));
  return preference.filter((name) => names.has(name));
}

export async function negotiate(url: string, init: { headers: Headers; credentials: RequestCredentials; fetchImpl: typeof fetch; signal?: AbortSignal }): Promise<Negotiation> {
  let response: Response;
  try {
    response = await init.fetchImpl(join(url, "negotiate"), {
      method: "POST",
      headers: init.headers,
      credentials: init.credentials,
      signal: init.signal,
      body: "{}",
    });
  } catch (cause) {
    throw new RealtimeError("negotiation_failed", "negotiation request failed", { status: 0, cause });
  }
  if (!response.ok) {
    const err = httpFailure(response.status, "negotiation");
    if (err.code === "transport_failed") {
      throw new RealtimeError("negotiation_failed", "negotiation was rejected", { status: response.status });
    }
    throw err;
  }
  let body: unknown;
  try {
    body = await response.json();
  } catch (cause) {
    throw new RealtimeError("protocol", "negotiation body is not JSON", { cause });
  }
  return parseNegotiation(body);
}

export function join(base: string, suffix: string): string {
  return `${base.replace(/\/+$/, "")}/${suffix}`;
}

export function toWebSocketURL(httpURL: string): string {
  const url = new URL(httpURL);
  if (url.protocol === "https:") {
    url.protocol = "wss:";
  } else if (url.protocol === "http:") {
    url.protocol = "ws:";
  } else {
    throw new RealtimeError("lifecycle", "url must be http or https");
  }
  return url.toString();
}
