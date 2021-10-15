package config

type Config struct {
	Type   string `json:"type"`
	Server struct {
		ListenAddr string `json:"listen_addr"`
		Token      string `json:"token"`
		CertPath   string `json:"cert_path"`
		KeyPath    string `json:"key_path"`
	} `json:"server"`

	Client struct {
		Token string `json:"token"`

		RemoteAddr string `json:"remote_addr"`
		LocalAddr  string `json:"local_addr"`
	} `json:"client"`
}
