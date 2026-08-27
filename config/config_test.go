package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadLegacyConfigDefaults(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "frp.json")
	contents := `{
  "type": "client",
  "token": "test-token",
  "client": {
    "remote_addr": "example.com:443",
    "control_server_name": "example.com"
  }
}`
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadConfig(configPath); err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if C.Transport != TransportLegacyTLS {
		t.Fatalf("Transport = %q, want %q", C.Transport, TransportLegacyTLS)
	}
	if C.Client.WebSocketPath != DefaultWebSocketPath {
		t.Fatalf("WebSocketPath = %q, want %q", C.Client.WebSocketPath, DefaultWebSocketPath)
	}
}

func TestValidateWSSConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name: "valid server",
			config: Config{
				Type:      "server",
				Transport: TransportWSSMux,
				Token:     "test-token",
				Server: ServerConfig{
					ListenAddr:        "127.0.0.1:4002",
					ControlServerName: "tunnel.example.com",
					WebSocketPath:     DefaultWebSocketPath,
					ForwardServers: []ForwardServerConfig{{
						ProxyServerName: "rss.example.com",
						ListenAddr:      "127.0.0.1:18001",
					}},
				},
			},
		},
		{
			name: "public WSS listener",
			config: Config{
				Type:      "server",
				Transport: TransportWSSMux,
				Token:     "test-token",
				Server: ServerConfig{
					ListenAddr:        "0.0.0.0:4002",
					ControlServerName: "tunnel.example.com",
					WebSocketPath:     DefaultWebSocketPath,
					ForwardServers: []ForwardServerConfig{{
						ProxyServerName: "rss.example.com",
						ListenAddr:      "127.0.0.1:18001",
					}},
				},
			},
			wantErr: "must be a loopback address",
		},
		{
			name: "remote plaintext websocket",
			config: Config{
				Type:      "client",
				Transport: TransportWSSMux,
				Token:     "test-token",
				Client: ClientConfig{
					RemoteAddr:        "203.0.113.1:443",
					ControlServerName: "tunnel.example.com",
					WebSocketPath:     DefaultWebSocketPath,
					WebSocketScheme:   "ws",
					LocalServers: []LocalServerConfig{{
						ProxyServerName: "rss.example.com",
						LocalAddr:       "127.0.0.1:8080",
					}},
				},
			},
			wantErr: "restricted to loopback",
		},
		{
			name: "duplicate service",
			config: Config{
				Type:      "server",
				Transport: TransportWSSMux,
				Token:     "test-token",
				Server: ServerConfig{
					ListenAddr:        "127.0.0.1:4002",
					ControlServerName: "tunnel.example.com",
					WebSocketPath:     DefaultWebSocketPath,
					ForwardServers: []ForwardServerConfig{
						{ProxyServerName: "rss.example.com", ListenAddr: "127.0.0.1:18001"},
						{ProxyServerName: "rss.example.com", ListenAddr: "127.0.0.1:18002"},
					},
				},
			},
			wantErr: "duplicate forward server",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestExampleConfigs(t *testing.T) {
	for _, name := range []string{"wss-server.json", "wss-client.json"} {
		t.Run(name, func(t *testing.T) {
			if err := LoadConfig(filepath.Join("..", "examples", name)); err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if C.Transport != TransportWSSMux {
				t.Fatalf("Transport = %q, want %q", C.Transport, TransportWSSMux)
			}
		})
	}
}
