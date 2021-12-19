package main

import (
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"frp/client"
	"frp/config"
	"frp/server"
)

type service interface {
	Start() error
	Stop()
}

func run(cli *cli.Context) error {
	configPath := "./frp.json"
	if cli.IsSet("config") {
		configPath = cli.String("config")
	}
	err := config.LoadConfig(configPath)
	if err != nil {
		return errors.WithStack(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var s service
	if config.C.Type == "server" {
		s = server.New()
	} else {
		s = client.New()
	}
	if err := s.Start(); err != nil {
		return errors.WithStack(err)
	}

	sig := <-sigs
	log.Infof("receive signal %s to exit", sig.String())
	s.Stop()
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "frp"
	app.Usage = "frp"
	app.Flags = []cli.Flag{&cli.StringFlag{
		Name:     "config",
		Aliases:  []string{"c"},
		Value:    "./frp.json",
		Usage:    "config file path",
		Required: false,
	}}
	app.Action = run
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func init() {
	log.SetFormatter(&log.TextFormatter{})
	rand.Seed(time.Now().UnixNano())
}
