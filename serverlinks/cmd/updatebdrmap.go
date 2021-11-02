package main

import (
	"io/ioutil"
	"log"
	"path/filepath"
	"regexp"
	"serverlinks/bdrmaplink"
	"serverlinks/config"
	"serverlinks/fileutils"
	"spservers/spdb"
	"sync"
)

var Worker int = 5

func main() {
	bdrlnkconfig, help := config.ReadBdrConfig()
	if help {
		return
	}
	var wg sync.WaitGroup
	workerch := make(chan int, Worker)
	vmnamere := regexp.MustCompile(`(\w+-\w+-\w+)`)
	resultfolder, err := ioutil.ReadDir(bdrlnkconfig.ResultDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range resultfolder {
		if f.IsDir() {
			log.Println("fname", f.Name())
			nameslice := vmnamere.FindStringSubmatch(f.Name())
			if len(nameslice) > 0 {
				wg.Add(1)
				workerch <- 1
				go processVMbdrmap(nameslice[1], bdrlnkconfig, &wg, workerch)
			}
		}
	}
	wg.Wait()
	//if there is a newer bdrmap file, compute the links and load into db
}

func processVMbdrmap(vmname string, config *config.BdrConfig, wg *sync.WaitGroup, workerch chan int) {
	defer wg.Done()
	vmpath := filepath.Join(config.ResultDir, vmname)
	log.Println("working on", vmpath)
	/*vmbdrfiles, err := ioutil.ReadDir(vmpath)
	if err != nil {
		log.Panic(err)
	}*/
	monbdrstatus, err := config.MongoClient.QueryDataStatus(vmname)
	if err != nil {
		log.Panic(err)
	}
	log.Println(monbdrstatus)
	//no files in directory
	//sort file names by desc order of timestamp in the filename
	//only consider tar.bz2 file here. other files will set ts as 0
	vmbdrfiles, err := fileutils.SortResultFiles(vmpath, bdrmaplink.ParseBdrmapFileTs, 0)
	if len(vmbdrfiles) == 0 {
		return
	}
	/*	sort.Slice(vmbdrfiles, func(i, j int) bool {
			its := bdrmaplink.ParseBdrmapFileTs(vmbdrfiles[i].Name())
			jts := bdrmaplink.ParseBdrmapFileTs(vmbdrfiles[j].Name())
			//		log.Println(vmbdrfiles[i].Name(), its, vmbdrfiles[j].Name(), jts)
			return its > jts
		})
		for vidx, v := range vmbdrfiles {
			log.Println(vidx, v.Name())
		}*/
	var lastts int64 = 0
	if len(monbdrstatus.BdrmapFile) > 0 {
		//exist record in mongodb
		lastts = bdrmaplink.ParseBdrmapFileTs(monbdrstatus.BdrmapFile)
	}
	//double check if it is a tar.bz2 file
	var curfileidx int
	for curfileidx = 0; curfileidx < len(vmbdrfiles); curfileidx++ {
		newbdrts := bdrmaplink.ParseBdrmapFileTs(vmbdrfiles[curfileidx])
		log.Println("new:", newbdrts, lastts)
		if newbdrts > 0 && newbdrts > lastts {
			bresult := config.PrepareData(vmbdrfiles[curfileidx])
			if bresult != nil {
				linkmap := make(map[string]*spdb.Link)
				faripmap := make(map[string][]*spdb.Link)
				bdrmaplink.GenerateLinks(config, bresult, linkmap, faripmap)
				config.MongoClient.UpdateLinkstoMongo(vmname, newbdrts, linkmap)
				monbdrstatus.Mon = vmname
				monbdrstatus.BdrmapFile = filepath.Base(vmbdrfiles[curfileidx])
				config.MongoClient.UpdateDataStatus(monbdrstatus)
				lastts = newbdrts
				bresult.CleanupTmp()
			} else {
				log.Println("bdrmapfile invalid", vmname)
			}
		}
	}
	<-workerch
}
