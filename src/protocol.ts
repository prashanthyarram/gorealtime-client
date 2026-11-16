import { RealtimeError } from "./errors.js";

/** Envelope version this client speaks. Matches the Go server. */
export const PROTOCOL_VERSION = 1;

export const DEFAULT_MAX_PAYLOAD_BYTES = 32 * 1024;

const TOKEN = /^[A-Za-z0-9._:-]{1,128}$/;
const ID_TOKEN = /^[A-Za-z0-9._:-]{1,64}$/;

export interface ErrorBody {
  code: string;
  message: string;
}

export interface Envelope {
  v: number;
  type: string;
  event?: string;
  id?: string;
  corr?: string;
  origin?: string;
  trace?: Record<string, string>;
  payload?: unknown;
  error?: ErrorBody;
}

export function isToken(value: string, max: number): boolean {
  if (value.length < 1 || value.length > max) {
    return false;
  }
  return (max === 64 ? ID_TOKEN : TOKEN).test(value);
}

export function validateEventName(name: string): void {
  if (!isToken(name, 128)) {
    throw new RealtimeError("serialization", "event name is not a valid token");
  }
}

/** Encodes one client event or ack. Payload is embedded once, not stringified twice. */
export function encodeClient(env: Envelope, maxPayloadBytes: number): string {
  if (env.type !== "event" && env.type !== "ack") {
    throw new RealtimeError("serialization", "client frames must be event or ack");
  }
  if (env.type === "event") {
    validateEventName(env.event ?? "");
  }
  if (env.type === "ack" && !env.corr) {
    throw new RealtimeError("serialization", "ack requires corr");
  }
  if (env.id !== undefined && !isToken(env.id, 64)) {
    throw new RealtimeError("serialization", "frame id is not a valid token");
  }
  if (env.corr !== undefined && !isToken(env.corr, 64)) {
    throw new RealtimeError("serialization", "correlation id is not a valid token");
  }
  const body: Envelope = {
    v: PROTOCOL_VERSION,
    type: env.type,
    event: env.event,
    id: env.id,
    corr: env.corr,
  };
  if (env.payload !== undefined) {
    body.payload = env.payload;
  }
  let text: string;
  try {
    text = JSON.stringify(body);
  } catch (cause) {
    throw new RealtimeError("serialization", "payload is not JSON", { cause });
  }
  if (env.payload !== undefined) {
    const payloadText = JSON.stringify(env.payload);
    if (new TextEncoder().encode(payloadText).length > maxPayloadBytes) {
      throw new RealtimeError("serialization", "payload exceeds the configured limit");
    }
  }
  return text;
}

/** Parses one server frame. Throws {@link RealtimeError} with code `protocol` when the frame is unusable. */
export function decodeServer(text: string): Envelope {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (cause) {
    throw new RealtimeError("protocol", "frame is not JSON", { cause });
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new RealtimeError("protocol", "frame is not an object");
  }
  const raw = value as Record<string, unknown>;
  if (raw.v !== PROTOCOL_VERSION) {
    throw new RealtimeError("protocol", "unsupported protocol version");
  }
  if (typeof raw.type !== "string" || raw.type.length === 0) {
    throw new RealtimeError("protocol", "frame type is missing");
  }
  const env: Envelope = { v: PROTOCOL_VERSION, type: raw.type };
  if (raw.event !== undefined) {
    if (typeof raw.event !== "string") {
      throw new RealtimeError("protocol", "event name is invalid");
    }
    env.event = raw.event;
  }
  if (raw.id !== undefined) {
    if (typeof raw.id !== "string") {
      throw new RealtimeError("protocol", "frame id is invalid");
    }
    env.id = raw.id;
  }
  if (raw.corr !== undefined) {
    if (typeof raw.corr !== "string") {
      throw new RealtimeError("protocol", "correlation id is invalid");
    }
    env.corr = raw.corr;
  }
  if (raw.origin !== undefined && typeof raw.origin === "string") {
    env.origin = raw.origin;
  }
  if (raw.payload !== undefined) {
    env.payload = raw.payload;
  }
  if (raw.error !== undefined) {
    if (raw.error === null || typeof raw.error !== "object") {
      throw new RealtimeError("protocol", "error body is invalid");
    }
    const err = raw.error as Record<string, unknown>;
    env.error = {
      code: typeof err.code === "string" ? err.code : "",
      message: typeof err.message === "string" ? err.message : "",
    };
  }
  if (env.type === "event" && (env.event === undefined || !isToken(env.event, 128))) {
    throw new RealtimeError("protocol", "event name is invalid");
  }
  if (env.type === "hello") {
    const payload = env.payload;
    const id =
      payload !== null && typeof payload === "object" && !Array.isArray(payload)
        ? (payload as Record<string, unknown>).connectionId
        : undefined;
    if (typeof id !== "string" || id.length === 0) {
      throw new RealtimeError("protocol", "hello is missing connectionId");
    }
  }
  return env;
}

export function newFrameId(): string {
  const cryptoObj = globalThis.crypto;
  if (cryptoObj?.randomUUID) {
    return cryptoObj.randomUUID();
  }
  const bytes = new Uint8Array(16);
  cryptoObj.getRandomValues(bytes);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}
