package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/pkg/errors"
)

var (
	// MagicNumber is magic number
	MagicNumber uint64 = 235300467370941978

	// C is the applications config
	C *Config = new(Config)

	// MagicNumberNotEqual define error for magic number not equal
	MagicNumberNotEqual = errors.Errorf("magic number not equal")
)

const (
	// TransportLegacyTLS remains the default so existing configs keep working.
	TransportLegacyTLS = "legacy_tls"
	// TransportWSSMux avoids one externally visible TLS connection per service.
	TransportWSSMux = "wss_mux"
	// DefaultWebSocketPath keeps both peers compatible when the field is omitted.
	DefaultWebSocketPath = "/api/v1/events"
)

// ForwardServerConfig keeps public routing outside the authenticated tunnel.
type ForwardServerConfig struct {
	ProxyServerName string `json:"proxy_server_name"`
	ListenAddr      string `json:"listen_addr"`
	CertPath        string `json:"cert_path"`
	KeyPath         string `json:"key_path"`
}

// LocalServerConfig prevents the relay from supplying arbitrary origin addresses.
type LocalServerConfig struct {
	ProxyServerName string `json:"proxy_server_name"`
	LocalAddr       string `json:"local_addr"`
}

// ServerConfig retains legacy fields so deployments can roll back by configuration.
type ServerConfig struct {
	ListenAddr        string                `json:"listen_addr"`
	ControlServerName string                `json:"control_server_name"`
	WebSocketPath     string                `json:"websocket_path"`
	CertPath          string                `json:"cert_path"`
	KeyPath           string                `json:"key_path"`
	LocalHTTPAddr     string                `json:"local_http_addr"`
	ForwardServers    []ForwardServerConfig `json:"forward_servers"`
}

// ClientConfig retains legacy fields so deployments can roll back by configuration.
type ClientConfig struct {
	RemoteAddr        string              `json:"remote_addr"`
	ControlServerName string              `json:"control_server_name"`
	WebSocketPath     string              `json:"websocket_path"`
	WebSocketScheme   string              `json:"websocket_scheme"`
	LocalServers      []LocalServerConfig `json:"local_servers"`
}

// Config define the config
type Config struct {
	Type        string `json:"type"`
	Transport   string `json:"transport"`
	MagicNumber uint64 `json:"magic_number"`
	Token       string `json:"token"`
	LogLevel    string `json:"log_level"`

	Server ServerConfig `json:"server"`
	Client ClientConfig `json:"client"`
}

// LoadConfig loads the json config
func LoadConfig(configPath string) (err error) {
	bytes, err := os.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, "open config file failed")
	}

	loaded := new(Config)
	if err = json.Unmarshal(bytes, loaded); err != nil {
		return errors.Wrap(err, "parse config failed")
	}
	loaded.setDefaults()
	if err = loaded.Validate(); err != nil {
		return errors.Wrap(err, "validate config failed")
	}
	C = loaded

	if C.MagicNumber != 0 {
		MagicNumber = C.MagicNumber
	}

	return nil
}

func (c *Config) setDefaults() {
	if c.Transport == "" {
		c.Transport = TransportLegacyTLS
	}
	if c.Server.WebSocketPath == "" {
		c.Server.WebSocketPath = DefaultWebSocketPath
	}
	if c.Client.WebSocketPath == "" {
		c.Client.WebSocketPath = DefaultWebSocketPath
	}
	if c.Client.WebSocketScheme == "" {
		c.Client.WebSocketScheme = "wss"
	}
}

// Validate rejects ambiguous routing and unsafe WSS client settings.
func (c *Config) Validate() error {
	if c.Type != "server" && c.Type != "client" {
		return fmt.Errorf("type must be server or client")
	}
	if c.Token == "" {
		return fmt.Errorf("token must not be empty")
	}
	if c.Transport != TransportLegacyTLS && c.Transport != TransportWSSMux {
		return fmt.Errorf("unsupported transport %q", c.Transport)
	}
	if c.Transport == TransportLegacyTLS {
		return nil
	}
	if c.Type == "server" {
		return validateWSSServer(c.Server)
	}
	return validateWSSClient(c.Client)
}

func validateWSSServer(server ServerConfig) error {
	if err := validateAddress("server.listen_addr", server.ListenAddr); err != nil {
		return err
	}
	if !isLoopbackAddress(server.ListenAddr) {
		return fmt.Errorf("server.listen_addr must be a loopback address")
	}
	if server.ControlServerName == "" {
		return fmt.Errorf("server.control_server_name must not be empty")
	}
	if !strings.HasPrefix(server.WebSocketPath, "/") {
		return fmt.Errorf("server.websocket_path must start with /")
	}
	if len(server.ForwardServers) == 0 {
		return fmt.Errorf("server.forward_servers must not be empty")
	}
	seenNames := make(map[string]struct{}, len(server.ForwardServers))
	seenAddresses := make(map[string]struct{}, len(server.ForwardServers))
	for _, forward := range server.ForwardServers {
		if forward.ProxyServerName == "" {
			return fmt.Errorf("forward server name must not be empty")
		}
		if _, ok := seenNames[forward.ProxyServerName]; ok {
			return fmt.Errorf("duplicate forward server %q", forward.ProxyServerName)
		}
		seenNames[forward.ProxyServerName] = struct{}{}
		if err := validateAddress("forward listen_addr", forward.ListenAddr); err != nil {
			return err
		}
		if !isLoopbackAddress(forward.ListenAddr) {
			return fmt.Errorf("forward listen_addr must be a loopback address")
		}
		if _, ok := seenAddresses[forward.ListenAddr]; ok {
			return fmt.Errorf("duplicate forward listen_addr %q", forward.ListenAddr)
		}
		seenAddresses[forward.ListenAddr] = struct{}{}
	}
	return nil
}

func validateWSSClient(client ClientConfig) error {
	if err := validateAddress("client.remote_addr", client.RemoteAddr); err != nil {
		return err
	}
	if client.ControlServerName == "" {
		return fmt.Errorf("client.control_server_name must not be empty")
	}
	if !strings.HasPrefix(client.WebSocketPath, "/") {
		return fmt.Errorf("client.websocket_path must start with /")
	}
	if client.WebSocketScheme != "wss" && client.WebSocketScheme != "ws" {
		return fmt.Errorf("client.websocket_scheme must be wss or ws")
	}
	if client.WebSocketScheme == "ws" && !isLoopbackAddress(client.RemoteAddr) {
		return fmt.Errorf("unencrypted ws transport is restricted to loopback testing")
	}
	if len(client.LocalServers) == 0 {
		return fmt.Errorf("client.local_servers must not be empty")
	}
	seen := make(map[string]struct{}, len(client.LocalServers))
	for _, local := range client.LocalServers {
		if local.ProxyServerName == "" {
			return fmt.Errorf("local server name must not be empty")
		}
		if _, ok := seen[local.ProxyServerName]; ok {
			return fmt.Errorf("duplicate local server %q", local.ProxyServerName)
		}
		seen[local.ProxyServerName] = struct{}{}
		if err := validateAddress("local_addr", local.LocalAddr); err != nil {
			return err
		}
	}
	return nil
}

func validateAddress(field, address string) error {
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("%s %q is invalid: %w", field, address, err)
	}
	return nil
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
