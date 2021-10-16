package main

import (
	"frp/config"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func run(cli *cli.Context) error {
	configPath := "./frp.config"
	if cli.IsSet("config") {
		configPath = cli.String("config")
	}
	err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "frp"
	app.Usage = "frp"
	app.Flags = []cli.Flag{&cli.StringFlag{
		Name:     "config",
		Aliases:  []string{"c"},
		Value:    "./frp.config",
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
}
