package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"serverlinks/bdrmaplink"
	"serverlinks/config"
	"serverlinks/iputils"
	"serverlinks/sptraceroute"
)

var Param *config.Config

func main() {
	defer log.Println("End")
	//var wg sync.WaitGroup
	linkmap := make(map[string]*bdrmaplink.Link)
	faripmap := make(map[string][]*bdrmaplink.Link)
	servermap := make(map[string]*sptraceroute.ServerLink)
	if Param, exit := config.ReadConfig(); exit {
		return
	}
	log.Println("Tempdir", Param.Tmpdir)
	if Param.Cleanup {
		defer Cleanup(Param.Tmpdir)
	}
	PrepareData()
	//if the link file exist, we do not update the archive
	if Param.AddResult && len(Param.LinkFile) == 0 {
		defer StoreResult()
	}
	iphandler := iputils.NewIPHandler(Param.Prefix2ASFile)
	GenerateLinks(linkmap, faripmap)
	sptraceroute.ParseServerTrace(Param, linkmap, faripmap, servermap, iphandler, sptraceroute.Comcast)
	sptraceroute.ParseServerTrace(Param, linkmap, faripmap, servermap, iphandler, sptraceroute.Ookla)
	sptraceroute.ParseServerTrace(Param, linkmap, faripmap, servermap, iphandler, sptraceroute.Ndt)
	matchserver := 0
	for _, s := range servermap {
		matchserver++
		serveras, _, _ := prefixtree.GetByString(s.ServerIP)
		fmt.Printf("%d Server: %s ServerAS: %s, Near: %s Far: %s Far AS %s\n", s.Type, s.ServerIP, serveras, s.Lnk.NearIP, s.Lnk.FarIP, s.Lnk.FarAS)
	}
	coverlink := 0
	for _, l := range linkmap {
		if l.Covered {
			coverlink++
		}
	}
	log.Println("Match Server:", matchserver, " Covered link:", coverlink, "Total Link:", len(faripmap))
	ExportServerLink(servermap)
}
func Cleanup(tmpdir string) {
	log.Println("Cleaning up")
	os.RemoveAll(tmpdir)
}

func StoreResult() {
	log.Println("Storing results into result archive. Please wait...")
	storecmd := exec.Command("tar", "cjf", Param.ResultFile, "-C", Param.Tmpdir, filepath.Base(Param.LinkFile), filepath.Base(Param.RTRFile), filepath.Base(Param.AliasesFile), "results")
	err := storecmd.Run()
	if err != nil {
		log.Panic(err)
	}
}

func ExportServerLink(sl map[string]*sptraceroute.ServerLink) {
	//assume result files is .tar.bz2 lenght to cut=8
	slfilepath := Param.ResultFile[:len(Param.ResultFile)-8] + ".gob"
	log.Println("Exporting Gob:", slfilepath)
	slfile, err := os.Create(slfilepath)
	if err != nil {
		log.Panic(err)
	}
	slencoder := gob.NewEncoder(slfile)
	slencoder.Encode(sl)
	slfile.Close()
}
