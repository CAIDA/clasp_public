package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"mmbot"
	"os"
	"os/exec"
	"path/filepath"
	"spservers/spdb"
	"strings"
)

type BdrConfig struct {
	ScamperBin       string
	ResultDir        string
	SiblingDir       string
	Prefix2ASDir     string
	PeeringDir       string
	DelegationDir    string
	ASRelDir         string
	MongoConfig      string
	MattermostConfig string
	//	SpeedTestServersDir string
	Quiet       bool
	Cleanup     bool
	Clean       bool
	AddResult   bool
	MMclient    *mmbot.MMBot
	MongoClient *spdb.SpeedtestMongo
}

type BdrResult struct {
	MetaFile         string
	BdrWartsFile     string
	NDTWartsFile     string
	OoklaWartsFile   string
	ComcastWartsFile string
	SiblingFile      string
	Prefix2ASFile    string
	PeeringFile      string
	DelegationFile   string
	ASRelFile        string
	//	OoklaServerFile     string
	//	NDTServerFile       string
	//	ComcastServerFile   string
	RTRFile     string
	LinkFile    string
	AliasesFile string
	Tmpdir      string
}

const PROJECTDIR = "/scratch/cloudspeedtest"
const GETLINKSCRIPT = PROJECTDIR + "/analysis/scripts/get_rtr_links_bdrmap.py"

func ReadBdrConfig() (*BdrConfig, bool) {
	Param := &BdrConfig{}
	help := false
	flag.StringVar(&Param.ScamperBin, "scamper", filepath.Join(PROJECTDIR, "bin/scamper/bin/"), "path to scamper util binaries")
	flag.StringVar(&Param.ResultDir, "r", filepath.Join(PROJECTDIR, "result/bdrmap"), "path to result file to be analyze (assume in tar.bz format)")
	flag.StringVar(&Param.SiblingDir, "sb", filepath.Join(PROJECTDIR, "analysis/sibling"), "directory that contains sibling files")
	flag.StringVar(&Param.Prefix2ASDir, "pfx", filepath.Join(PROJECTDIR, "analysis/prefix2as"), "directory that contains prefix2as files")
	flag.StringVar(&Param.PeeringDir, "ixp", filepath.Join(PROJECTDIR, "analysis/peering"), "directory that contains peering (ixp) files")
	flag.StringVar(&Param.DelegationDir, "d", filepath.Join(PROJECTDIR, "analysis/delegation/"), "directory that contains delegation files")
	flag.StringVar(&Param.ASRelDir, "a", filepath.Join(PROJECTDIR, "analysis/as-rel/"), "directory that contains AS relationship files")
	flag.StringVar(&Param.MongoConfig, "db", filepath.Join(PROJECTDIR, "bin/beamermongosp.json"), "path to mongodb information")
	flag.StringVar(&Param.MattermostConfig, "mm", filepath.Join(PROJECTDIR, "bin/mattermostbot.json"), "path to mattermost bot config file")
	//flag.StringVar(&Param.SpeedTestServersDir, "st", filepath.Join(PROJECTDIR, "result/spservers"), "directory that contains Speedtest servers information")
	flag.BoolVar(&Param.Cleanup, "x", true, "Delete tmp directory after analysis")
	flag.BoolVar(&Param.Quiet, "q", false, "Disable mattermost posting")
	flag.BoolVar(&Param.Clean, "c", false, "Force to regenerate router/link/alias files")
	flag.BoolVar(&Param.AddResult, "A", true, "Add router/link/alias files into original result archive. Note that original file will be replaced")
	flag.BoolVar(&help, "h", false, "Print this help")
	flag.Parse()
	if help {
		fmt.Println("This script extracts interdomain links from bdrmap runs and select speedtest servers.")
		flag.PrintDefaults()
		return nil, help
	}
	if _, err := os.Stat(Param.ResultDir); os.IsNotExist(err) {
		log.Panic("Result dir does not exist.", Param.ResultDir)
	}
	if _, err := os.Stat(Param.DelegationDir); os.IsNotExist(err) {
		log.Panic("Delegation Directory does not exist.", Param.DelegationDir)
	}
	if _, err := os.Stat(Param.ASRelDir); os.IsNotExist(err) {
		log.Panic("AS Relationship Directory does not exist.", Param.ASRelDir)
	}
	/*if _, err := os.Stat(Param.SpeedTestServersDir); os.IsNotExist(err) {
		log.Panic("SpeedTest Server Directory does not exist.", Param.SpeedTestServersDir)
	}*/
	if _, err := os.Stat(Param.SiblingDir); os.IsNotExist(err) {
		log.Panic("Sibling Directory does not exist.", Param.SiblingDir)
	}
	if _, err := os.Stat(Param.Prefix2ASDir); os.IsNotExist(err) {
		log.Panic("Prefix2AS Directory does not exist.", Param.Prefix2ASDir)
	}
	if _, err := os.Stat(Param.PeeringDir); os.IsNotExist(err) {
		log.Panic("Peering Directory does not exist.", Param.PeeringDir)
	}
	if _, err := os.Stat(Param.MongoConfig); os.IsNotExist(err) {
		log.Panic("Mongodb config file doest not exist", Param.MongoConfig)
	}
	if _, err := os.Stat(Param.MattermostConfig); os.IsNotExist(err) {
		log.Panic("Mattermost config file does not exist", Param.MattermostConfig)
	}

	//setup Mattermost
	Param.MMclient = mmbot.NewMMBot(Param.MattermostConfig)
	Param.MongoClient = spdb.NewMongoDB(Param.MongoConfig, "speedtest")

	//infer other file locations
	return Param, false
}

func (Param *BdrConfig) PrepareData(resultfile string) *BdrResult {
	bresult := &BdrResult{}
	tmpdir, err := ioutil.TempDir("./", "csp")
	if err != nil {
		log.Panic(err)
	}

	bresult.Tmpdir, _ = filepath.Abs(tmpdir)

	cmd := exec.Command("tar", "xjf", resultfile, "-C", bresult.Tmpdir)
	err = cmd.Run()
	if err != nil {
		log.Println("Decompress result file failed", err, resultfile)
		return nil
	}
	resultdir := filepath.Join(bresult.Tmpdir, "results/bdrmap")
	files, err := ioutil.ReadDir(resultdir)
	if err != nil {
		bresult.CleanupTmp()
		log.Println(err)
		return nil
	}
	for _, f := range files {
		if strings.Contains(f.Name(), "bdrmap.meta") {
			bresult.MetaFile = filepath.Join(resultdir, f.Name())
			continue
		}
		if strings.Contains(f.Name(), "bdrmap.warts") {
			bresult.BdrWartsFile = filepath.Join(resultdir, f.Name())
			continue
		}
		if strings.Contains(f.Name(), "comcast") {
			bresult.ComcastWartsFile = filepath.Join(resultdir, f.Name())
			continue
		}
		if strings.Contains(f.Name(), "ndt") {
			bresult.NDTWartsFile = filepath.Join(resultdir, f.Name())
			continue
		}
		if strings.Contains(f.Name(), "ookla") {
			bresult.OoklaWartsFile = filepath.Join(resultdir, f.Name())
			continue
		}
	}

	if _, err := os.Stat(bresult.BdrWartsFile); os.IsNotExist(err) {
		log.Println("Warts File does not exist.", bresult.BdrWartsFile)
		bresult.CleanupTmp()
		return nil
	}
	if !Param.Clean {
		//Check for router/link/alias files from previous runs
		rootfiles, err := ioutil.ReadDir(bresult.Tmpdir)
		if err != nil {
			log.Panic(err)
			return nil
		}
		for _, f := range rootfiles {
			if strings.Contains(f.Name(), "bdrmap.aliases.out") {
				bresult.AliasesFile = filepath.Join(bresult.Tmpdir, f.Name())
				continue
			}
			if strings.Contains(f.Name(), "bdrmap.links.out") {
				bresult.LinkFile = filepath.Join(bresult.Tmpdir, f.Name())
				continue
			}
			if strings.Contains(f.Name(), "bdrmap.router.txt") {
				bresult.RTRFile = filepath.Join(bresult.Tmpdir, f.Name())
				continue
			}
		}
	}
	/*	serverfiles, err := ioutil.ReadDir(Param.SpeedTestServersDir)
		if err != nil {
			log.Panic(err)
		}
		//pull the latest server files
		ndtts, ooklats, comcastts := time.Time{}, time.Time{}, time.Time{}
		for _, f := range serverfiles {
			if strings.Contains(f.Name(), "ndt") {
				if f.ModTime().After(ndtts) {
					Param.NDTServerFile = filepath.Join(Param.SpeedTestServersDir, f.Name())
					ndtts = f.ModTime()
					continue
				}
			}
			if strings.Contains(f.Name(), "comcast") {
				if f.ModTime().After(comcastts) {
					Param.ComcastServerFile = filepath.Join(Param.SpeedTestServersDir, f.Name())
					comcastts = f.ModTime()
					continue
				}
			}
			if strings.Contains(f.Name(), "ookla") {
				if f.ModTime().After(ooklats) {
					Param.OoklaServerFile = filepath.Join(Param.SpeedTestServersDir, f.Name())
					ooklats = f.ModTime()
					continue
				}
			}
		}
	*/
	mdata, err := ioutil.ReadFile(bresult.MetaFile)
	if err != nil {
		log.Panic("Failed to read Meta file:", bresult.MetaFile, err)
		bresult.CleanupTmp()
		return nil
	}
	mdataarr := strings.Split(strings.TrimSuffix(string(mdata), "\n"), ",")
	//expect: <unix timestamp>,<peering file>,<sibling file>,<prefix2as file>
	//example: 1586944209,/home/ubuntu/outdir/datafiles/202001.v4.peering,/home/ubuntu/outdir/datafiles/amazon.sibling.active,/home/ubuntu/outdir/datafiles/20200401.prefix2as
	if len(mdataarr) != 4 {
		log.Panic("Incorrect format in meta file")
		bresult.CleanupTmp()
		return nil
	}
	bresult.PeeringFile = filepath.Join(Param.PeeringDir, filepath.Base(mdataarr[1]))

	if _, err := os.Stat(bresult.PeeringFile); os.IsNotExist(err) {
		log.Panic("Peering File does not exist.", bresult.PeeringFile)
	}
	//remove active suffix, replace with txt
	tmpsib := filepath.Base(mdataarr[2])
	siblingext := filepath.Ext(mdataarr[2])
	bresult.SiblingFile = filepath.Join(Param.SiblingDir, tmpsib[:len(tmpsib)-len(siblingext)]+".txt")
	if _, err := os.Stat(bresult.SiblingFile); os.IsNotExist(err) {
		log.Panic("Sibling File does not exist.", bresult.SiblingFile)
		bresult.CleanupTmp()
		return nil
	}
	ip2as := filepath.Base(mdataarr[3])
	bresult.Prefix2ASFile = filepath.Join(Param.Prefix2ASDir, ip2as)
	if _, err := os.Stat(bresult.Prefix2ASFile); os.IsNotExist(err) {
		log.Panic("Prefix2AS File does not exist.", bresult.Prefix2ASFile)
		bresult.CleanupTmp()
		return nil
	}
	//find the AS relationship of the same month as Prefix2AS
	//assume yyyymmdd.prefix2as->yyyymmdd.as-rel.txt
	yrmondd := ip2as[:len(ip2as)-len(filepath.Ext(ip2as))]
	bresult.ASRelFile = filepath.Join(Param.ASRelDir, yrmondd+".as-rel.txt")
	if _, err := os.Stat(bresult.ASRelFile); os.IsNotExist(err) {
		log.Panic("AS relationship file does not exist.", bresult.ASRelFile)
		bresult.CleanupTmp()
		return nil
	}
	yrmon := yrmondd[:len(yrmondd)-2]
	bresult.DelegationFile = filepath.Join(Param.DelegationDir, "delegated-ipv4-"+yrmon+".txt")
	if _, err := os.Stat(bresult.DelegationFile); os.IsNotExist(err) {
		log.Panic("Delegation file does not exist.", bresult.DelegationFile)
		bresult.CleanupTmp()
		return nil
	}
	return bresult
}

func (b *BdrResult) CleanupTmp() {
	os.RemoveAll(b.Tmpdir)
}
