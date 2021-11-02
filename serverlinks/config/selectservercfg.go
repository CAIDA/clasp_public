package config

import (
	"flag"
	"log"
	"mmbot"
	"os"
	"path/filepath"
	"spservers/spdb"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SsConfig struct {
	OutputDir        string
	MongoConfig      string
	MattermostConfig string
	Worker           int
	TargetperVM      int
	MaxVMperRegion   int
	MinTrThreshold   int
	RttThreshold     float64
	StartDate        time.Time
	EnableDate       time.Time
	MMclient         *mmbot.MMBot
	MongoClient      *spdb.SpeedtestMongo
}

type SsResult struct {
	Region     string
	LinkId     primitive.ObjectID
	SpServerId primitive.ObjectID
	/*	Linkkey      string
		FarIp        string
		SpIdentifier string*/
	Reason int
	Freq   int
	AvgRtt float64
}

func ReadSsConfig() *SsConfig {
	cfg := &SsConfig{}
	sttime := time.Now().AddDate(0, 0, -7)
	sts := sttime.Unix()
	ets := time.Now().Unix()
	flag.StringVar(&cfg.OutputDir, "o", "./", "path to output results")
	flag.StringVar(&cfg.MongoConfig, "db", filepath.Join(PROJECTDIR, "bin/beamermongosp.json"), "path to mongodb info")
	flag.StringVar(&cfg.MattermostConfig, "mm", filepath.Join(PROJECTDIR, "bin/mattermostbot.json"), "path to mattermost bot config file")
	flag.IntVar(&cfg.Worker, "w", 10, "Number of workers")
	flag.IntVar(&cfg.TargetperVM, "t", 20, "measurement targets per VM")
	flag.IntVar(&cfg.MaxVMperRegion, "x", 9, "Maximum number of VM per region")
	flag.IntVar(&cfg.MinTrThreshold, "tr", 10, "Minimum number of traceroute required to be observed")
	flag.Float64Var(&cfg.RttThreshold, "rtt", 150.0, "Maximum RTT to be considered as a target")
	flag.Int64Var(&sts, "ts", sts, "Unix timestamp of start time")
	flag.Int64Var(&ets, "ets", ets, "Unix timestamp of enable time")
	flag.Parse()
	if _, err := os.Stat(cfg.OutputDir); os.IsNotExist(err) {
		log.Panic("Output directory does not exist")
	}
	if cfg.Worker <= 0 {
		cfg.Worker = 1
	}
	if cfg.TargetperVM <= 0 {
		cfg.TargetperVM = 1
	}
	if cfg.MaxVMperRegion <= 0 {
		cfg.MaxVMperRegion = 1
	}
	if cfg.MinTrThreshold <= 0 {
		cfg.MinTrThreshold = 1
	}
	if cfg.RttThreshold < 0 {
		cfg.RttThreshold = 1
	}
	cfg.StartDate = time.Unix(sts, 0)
	cfg.EnableDate = time.Unix(ets, 0)
	cfg.MMclient = mmbot.NewMMBot(cfg.MattermostConfig)
	cfg.MMclient.Username = "SelectServer Process"
	cfg.MongoClient = spdb.NewMongoDB(cfg.MongoConfig, "speedtest")
	return cfg
}
