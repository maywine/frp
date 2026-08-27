package main

import (
	"math/rand"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"frp/client"
	"frp/config"
	"frp/server"
)

var (
	logLevelMap = map[string]log.Level{
		"fatal": log.FatalLevel,
		"error": log.ErrorLevel,
		"warn":  log.WarnLevel,
		"info":  log.InfoLevel,
		"debug": log.DebugLevel,
		"trace": log.TraceLevel,
	}
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

	logLevel := config.C.LogLevel
	if len(logLevel) == 0 {
		logLevel = "info"
	}
	level, ok := logLevelMap[logLevel]
	if !ok {
		level = log.InfoLevel
		log.Warnf("unknown log level %s, using log level info", logLevel)
	}
	log.SetLevel(level)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	log.Info("service start")
	var s service
	if config.C.Transport == config.TransportWSSMux {
		if config.C.Type == "server" {
			s = server.NewWSS()
		} else {
			s = client.NewWSS()
		}
	} else {
		if config.C.Type == "server" {
			s = server.New()
		} else {
			s = client.New()
		}
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
	log.SetReportCaller(true)
	log.SetFormatter(&log.TextFormatter{
		DisableColors:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			return frame.Function, path.Base(frame.File)
		},
	})
	log.SetLevel(log.InfoLevel)
	rand.Seed(time.Now().UnixNano())
}
