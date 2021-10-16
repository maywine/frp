package config

import (
	"encoding/json"
	"io/ioutil"

	"github.com/pkg/errors"
)

var (
	// C is the applications config
	C *Config = new(Config)
)

// Config define the config
type Config struct {
	Type   string `json:"type"`
	Server struct {
		Token          string `json:"token"`
		ListenAddr     string `json:"listen_addr"`
		Host           string `json:"host"`
		CertPath       string `json:"cert_path"`
		KeyPath        string `json:"key_path"`
		ForwardServers []struct {
			ServerName string `json:"server_name"`
			CertPath   string `json:"cert_path"`
			KeyPath    string `json:"key_path"`
		} `json:"forward_servers"`
	} `json:"server"`

	Client struct {
		Token        string `json:"token"`
		RemoteAddr   string `json:"remote_addr"`
		RemoteHost   string `json:"remote_host"`
		LocalServers []struct {
			ServerName string `json:"server_name"`
			LocalAddr  string `json:"local_addr"`
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

	return nil
}
