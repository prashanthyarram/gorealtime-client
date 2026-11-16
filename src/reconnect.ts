export interface RetryOptions {
  /** First delay before a retry, in milliseconds. Default 1000. */
  initialDelayMs?: number;
  /** Cap for the exponential delay. Default 30000. */
  maxDelayMs?: number;
  /** Fraction of the delay added or removed at random. Default 0.2. */
  jitter?: number;
  /** How many reconnect attempts follow a drop. Default 5. */
  maxRetries?: number;
}

export const defaultRetry: Required<RetryOptions> = {
  initialDelayMs: 1000,
  maxDelayMs: 30_000,
  jitter: 0.2,
  maxRetries: 5,
};

/** Exponential delay with symmetric jitter. `random` returns a value in [0, 1). */
export function retryDelay(attempt: number, options: RetryOptions | undefined, random: () => number): number {
  const cfg = { ...defaultRetry, ...options };
  const exp = Math.min(cfg.maxDelayMs, cfg.initialDelayMs * 2 ** attempt);
  const spread = exp * clamp(cfg.jitter, 0, 1);
  const value = exp - spread + random() * spread * 2;
  return Math.max(0, Math.round(value));
}

function clamp(n: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, n));
}
