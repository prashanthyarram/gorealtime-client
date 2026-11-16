import { RealtimeError } from "./errors.js";

const MAX_BUFFER = 1024 * 1024;

/**
 * Incremental SSE parser. One network chunk is not assumed to be one event.
 * The buffer is capped so a peer cannot grow it without bound.
 */
export function createSSEParser(onEvent: (data: string) => void, onComment?: (text: string) => void) {
  let buffer = "";
  return {
    push(chunk: string): void {
      buffer += chunk.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
      if (buffer.length > MAX_BUFFER) {
        buffer = "";
        throw new RealtimeError("protocol", "sse buffer exceeded the limit");
      }
      let split = buffer.indexOf("\n\n");
      while (split >= 0) {
        const block = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);
        dispatch(block, onEvent, onComment);
        split = buffer.indexOf("\n\n");
      }
    },
  };
}

function dispatch(block: string, onEvent: (data: string) => void, onComment?: (text: string) => void): void {
  const data: string[] = [];
  let comment = "";
  for (const line of block.split("\n")) {
    if (line.startsWith(":")) {
      comment = line.slice(1).trim();
      continue;
    }
    if (line.startsWith("data:")) {
      data.push(line.slice(5).replace(/^ /, ""));
    }
  }
  if (comment && onComment) {
    onComment(comment);
  }
  if (data.length > 0) {
    onEvent(data.join("\n"));
  }
}
