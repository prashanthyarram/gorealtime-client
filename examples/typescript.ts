import { RealtimeClient } from "@gorealtime/client";

interface ChatMessage {
  text: string;
  sender: string;
}

const client = new RealtimeClient({
  url: "https://api.example.com/realtime",
  automaticReconnect: true,
  accessTokenFactory: async () => sessionStorage.getItem("access-token") ?? undefined,
});

client.on<ChatMessage>("chat.message", (message) => {
  console.log(message.sender, message.text);
});

await client.connect();
await client.send("chat.send", { text: "Hello from TypeScript", sender: "ada" });
await client.disconnect();
