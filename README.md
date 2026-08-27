# frp

A small reverse tunnel for publishing services from a private origin through a
resource-constrained public relay.

## Transports

- `legacy_tls` keeps compatibility with the original per-service TLS protocol.
- `wss_mux` carries every configured service over one authenticated WebSocket
  and multiplexes logical streams with yamux.

For `wss_mux`, a reverse proxy must terminate public TLS. The server-side
WebSocket listener and every public service listener must remain bound to
loopback. The client rejects plaintext `ws` connections to non-loopback
addresses.

Example configurations are in `examples/wss-server.json`,
`examples/wss-client.json`, and `examples/nginx-wss.conf`.

## Validation

```sh
go test -race ./...
go vet ./...
go build ./...
```
