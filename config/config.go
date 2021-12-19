package config

import (
	"encoding/json"
	"io/ioutil"

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

// Config define the config
type Config struct {
	Type        string `json:"type"`
	MagicNumber uint64 `json:"magic_number"`
	Token       string `json:"token"`
	LogLevel    string `json:"log_level"`

	Server struct {
		ListenAddr        string `json:"listen_addr"`
		ControlServerName string `json:"control_server_name"`
		CertPath          string `json:"cert_path"`
		KeyPath           string `json:"key_path"`
		LocalHTTPAddr     string `json:"local_http_addr"`
		ForwardServers    []struct {
			ProxyServerName string `json:"proxy_server_name"`
			CertPath        string `json:"cert_path"`
			KeyPath         string `json:"key_path"`
		} `json:"forward_servers"`
	} `json:"server"`

	Client struct {
		RemoteAddr        string `json:"remote_addr"`
		ControlServerName string `json:"control_server_name"`
		LocalServers      []struct {
			ProxyServerName string `json:"proxy_server_name"`
			LocalAddr       string `json:"local_addr"`
		} `json:"local_servers"`
	} `json:"client"`
}

// LoadConfig loads the json config
func LoadConfig(configPath string) (err error) {
	bytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, "open config file failed")
	}

	if err = json.Unmarshal(bytes, C); err != nil {
		return errors.Wrap(err, "parse config failed")
	}

	if C.MagicNumber != 0 {
		MagicNumber = C.MagicNumber
	}

	return nil
}
