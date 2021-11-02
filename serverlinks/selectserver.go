package main

import (
	"flag"
	"io/ioutil"
	"log"
	"path/filepath"
	"serverlinks/sptraceroute"
)

func main() {
	gobpath := ""
	serverjsondir := ""
	outputpath := ""
	flag.StringVar(&gobpath, "i", "", "path and pattern to find gob files")
	flag.StringVar(&serverjsondir, "s", "/scratch/cloudspeedtest/testservers", "directory that contains speedtest server info")
	flag.StringVar(&outputpath, "o", "", "output result")
	flag.Parse()
	if len(gobpath) == 0 {
		flag.PrintDefaults()
		log.Panic("Must provide Gob path.")
	}
	serverinfo := sptraceroute.LoadServerInfo(serverjsondir)
	mergedlinks := sptraceroute.MergeServerLink(SelectGobFiles(gobpath))
	selectedservers := sptraceroute.SelectSPServerforLink(mergedlinks, serverinfo)
	sptraceroute.OutputServerList(selectedservers, outputpath, 2)
	//	sptraceroute.PrintServerList(selectedservers)
}

func SelectGobFiles(gobpath string) []string {
	gobfiles := []string{}
	gobdir := filepath.Dir(gobpath)
	gobdirfile, err := ioutil.ReadDir(gobdir)
	if err != nil {
		log.Panic(err)
	}
	fpattern := filepath.Base(gobpath)
	for _, f := range gobdirfile {
		if m, _ := filepath.Match(fpattern, f.Name()); m {
			gobfiles = append(gobfiles, filepath.Join(gobdir, f.Name()))
		}
	}
	return gobfiles
}
