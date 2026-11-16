/** Stable codes for {@link RealtimeError}. */
export type RealtimeErrorCode =
  | "negotiation_failed"
  | "unauthorized"
  | "forbidden"
  | "transport_failed"
  | "protocol"
  | "serialization"
  | "server"
  | "send_failed"
  | "disconnected"
  | "cancelled"
  | "reconnect_failed"
  | "lifecycle";

/**
 * Error raised by the client. `cause` is the underlying failure when there is one.
 * Messages never include tokens or header values.
 */
export class RealtimeError extends Error {
  readonly code: RealtimeErrorCode;
  readonly status?: number;

  constructor(code: RealtimeErrorCode, message: string, options?: { status?: number; cause?: unknown }) {
    super(message, options?.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = "RealtimeError";
    this.code = code;
    this.status = options?.status;
  }
}

/** Maps an HTTP status from negotiate or a transport open to a client error. */
export function httpFailure(status: number, where: string): RealtimeError {
  if (status === 401) {
    return new RealtimeError("unauthorized", `${where} was not authenticated`, { status });
  }
  if (status === 403) {
    return new RealtimeError("forbidden", `${where} was refused`, { status });
  }
  if (status === 400 || status === 409 || status === 413 || status === 426) {
    return new RealtimeError("protocol", `${where} rejected the request`, { status });
  }
  return new RealtimeError("transport_failed", `${where} failed`, { status });
}
