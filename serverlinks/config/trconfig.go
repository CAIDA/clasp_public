package config

import (
	"flag"
	"io/ioutil"
	"log"
	"mmbot"
	"os"
	"os/exec"
	"path/filepath"
	"serverlinks/iputils"
	"spservers/spdb"
	"strings"
)

type TrConfig struct {
	ScamperBin       string
	ResultDir        string
	Prefix2ASPathv4  string
	MongoConfig      string
	MattermostConfig string
	VMWorker         int
	TrWorker         int
	Cleanup          bool
	MMclient         *mmbot.MMBot
	MongoClient      *spdb.SpeedtestMongo
}

type TrResult struct {
	TraceTs          int64
	MetaFile         string
	NDTWartsFile     string
	OoklaWartsFile   string
	ComcastWartsFile string
	Tmpdir           string
	Prefix2As        iputils.IPHandler
}

func ReadTrConfig() *TrConfig {
	Param := &TrConfig{}
	flag.StringVar(&Param.ScamperBin, "scamper", filepath.Join(PROJECTDIR, "bin/scamper/bin/"), "path to scamper util binaries")
	flag.StringVar(&Param.ResultDir, "r", filepath.Join(PROJECTDIR, "result/trace"), "path to result file to be analyze (assume in tar.bz format)")
	flag.StringVar(&Param.Prefix2ASPathv4, "pfxv4", "", "path prefix to IPv4 prefix2as file")
	flag.StringVar(&Param.MongoConfig, "db", filepath.Join(PROJECTDIR, "bin/beamermongosp.json"), "path to mongodb information")
	flag.StringVar(&Param.MattermostConfig, "mm", filepath.Join(PROJECTDIR, "bin/mattermostbot.json"), "path to mattermost bot config file")
	flag.IntVar(&Param.VMWorker, "vw", 5, "Number of VM workers")
	flag.IntVar(&Param.TrWorker, "tw", 100, "Number of traceroute workers")
	flag.Parse()
	if _, err := os.Stat(Param.ScamperBin); os.IsNotExist(err) {
		log.Panic("scamper bin directory does not exist")
	}
	if _, err := os.Stat(Param.ResultDir); os.IsNotExist(err) {
		log.Panic("Result directory does not exist")
	}
	if Param.VMWorker <= 0 {
		Param.VMWorker = 1
	}
	if Param.TrWorker <= 0 {
		Param.TrWorker = 1
	}
	Param.MMclient = mmbot.NewMMBot(Param.MattermostConfig)
	Param.MongoClient = spdb.NewMongoDB(Param.MongoConfig, "speedtest")
	return Param
}

func (trconfig *TrConfig) PrepareTraceData(resultfile string) *TrResult {
	tresult := &TrResult{}
	tmpdir, err := ioutil.TempDir("./", "tr")
	if err != nil {
		log.Panic(err)
	}
	tresult.Tmpdir, _ = filepath.Abs(tmpdir)
	cmd := exec.Command("tar", "xjf", resultfile, "-C", tresult.Tmpdir)
	err = cmd.Run()
	if err != nil {
		log.Println("Decompress result file failed", err)
		return nil
	}

	resultdir := filepath.Join(tresult.Tmpdir)
	files, err := ioutil.ReadDir(resultdir)
	if err != nil {
		log.Panic("Read tmp dir failed")
	}
	for _, f := range files {
		if strings.Contains(f.Name(), "meta") {
			tresult.MetaFile = filepath.Join(resultdir, f.Name())
			continue
		}
		if strings.Contains(f.Name(), "ndt") {
			tresult.NDTWartsFile = filepath.Join(resultdir, f.Name())
			continue
		}
		if strings.Contains(f.Name(), "ookla") {
			tresult.OoklaWartsFile = filepath.Join(resultdir, f.Name())
			continue
		}
		if strings.Contains(f.Name(), "comcast") {
			tresult.ComcastWartsFile = filepath.Join(resultdir, f.Name())
			continue
		}
	}
	if len(tresult.MetaFile) == 0 || len(tresult.NDTWartsFile) == 0 || len(tresult.OoklaWartsFile) == 0 || len(tresult.ComcastWartsFile) == 0 {
		log.Println("Incomplete trace file", tresult)
		tresult.CleanupTmp()
		return nil
	}
	return tresult
}
func (b *TrResult) CleanupTmp() {
	os.RemoveAll(b.Tmpdir)
}
