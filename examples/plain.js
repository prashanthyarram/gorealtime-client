import { RealtimeClient } from "@gorealtime/client";

const client = new RealtimeClient({
  url: "https://api.example.com/realtime",
  automaticReconnect: true,
});

client.on("chat.message", (payload) => {
  console.log(payload);
});

await client.connect();
await client.send("chat.send", { text: "Hello from JavaScript" });
await client.disconnect();
