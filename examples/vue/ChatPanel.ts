import { onMounted, onUnmounted, ref } from "vue";
import { RealtimeClient } from "@gorealtime/client";

interface ChatMessage {
  text: string;
}

const client = new RealtimeClient({
  url: "https://api.example.com/realtime",
  automaticReconnect: true,
});

export function useChat() {
  const state = ref(client.state);
  const lines = ref<string[]>([]);

  onMounted(() => {
    client.on<ChatMessage>("chat.message", (message) => {
      lines.value = [...lines.value, message.text];
    });
    client.onLifecycle("connected", () => {
      state.value = client.state;
    });
    client.onLifecycle("disconnected", () => {
      state.value = client.state;
    });
    void client.connect();
  });

  onUnmounted(() => {
    client.off("chat.message");
    void client.disconnect();
  });

  function send() {
    void client.send("chat.send", { text: "Hello from Vue" });
  }

  return { state, lines, send };
}
