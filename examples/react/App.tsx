import { useEffect, useState } from "react";
import { RealtimeClient, type ConnectionState } from "@gorealtime/client";

interface ChatMessage {
  text: string;
}

const client = new RealtimeClient({
  url: "https://api.example.com/realtime",
  automaticReconnect: true,
});

export function ChatPanel() {
  const [state, setState] = useState<ConnectionState>(client.state);
  const [lines, setLines] = useState<string[]>([]);

  useEffect(() => {
    const stopMessage = client.on<ChatMessage>("chat.message", (message) => {
      setLines((current) => [...current, message.text]);
    });
    const stopState = client.onLifecycle("connected", () => setState(client.state));
    const stopDown = client.onLifecycle("disconnected", () => setState(client.state));
    void client.connect();
    return () => {
      stopMessage();
      stopState();
      stopDown();
      void client.disconnect();
    };
  }, []);

  return (
    <section>
      <p>{state}</p>
      <ul>
        {lines.map((line, index) => (
          <li key={index}>{line}</li>
        ))}
      </ul>
      <button type="button" onClick={() => void client.send("chat.send", { text: "Hello from React" })}>
        Send
      </button>
    </section>
  );
}
