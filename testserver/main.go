// Command testserver is a tiny gorealtime process for the TypeScript client.
// It is not part of the library. Tests start it and read the LISTEN line.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/prashanthyarram/gorealtime"
)

func main() {
	opts := []gorealtime.Option{gorealtime.WithPath("/realtime")}
	if names := strings.TrimSpace(os.Getenv("TRANSPORTS")); names != "" {
		opts = append(opts, gorealtime.WithTransports(split(names)...))
	}
	if secret := os.Getenv("REQUIRE_BEARER"); secret != "" {
		opts = append(opts,
			gorealtime.WithRequireAuth(),
			gorealtime.WithAuthenticator(func(_ context.Context, r *http.Request) (gorealtime.AuthResult, error) {
				if r.Header.Get("Authorization") != "Bearer "+secret {
					return gorealtime.AuthResult{}, gorealtime.ErrUnauthorized
				}
				return gorealtime.AuthResult{Identity: "user"}, nil
			}),
		)
	}
	srv := gorealtime.NewServer(opts...)
	srv.On("echo", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		return conn.Send(ctx, "echo", ev.Payload)
	})
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("LISTEN http://%s/realtime\n", ln.Addr().String())
	os.Stdout.Sync()
	if err := http.Serve(ln, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func split(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
