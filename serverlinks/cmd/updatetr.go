package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"serverlinks/config"
	"serverlinks/fileutils"
	"serverlinks/iputils"
	"serverlinks/sptraceroute"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	GlobalStart = 1588291200
)

func main() {
	trconfig := config.ReadTrConfig()
	trconfig.MMclient.Username = "Traceroute Updater"
	defer trconfig.MongoClient.Close()
	var wg sync.WaitGroup
	workerchan := make(chan int, trconfig.VMWorker)
	vmnamere := regexp.MustCompile(`(\w+-\w+-\w+)`)
	resultfolder, err := ioutil.ReadDir(trconfig.ResultDir)
	if err != nil {
		log.Fatal(err)
	}
	vmlist := []string{}
	for _, f := range resultfolder {
		if f.IsDir() {
			nameslice := vmnamere.FindStringSubmatch(f.Name())
			if len(nameslice) > 0 {
				vmlist = append(vmlist, nameslice[1])
				wg.Add(1)
				go func() {
					workerchan <- 1
					processVMTr(nameslice[1], trconfig)
					<-workerchan
					wg.Done()
				}()

			}
		}
	}
	wg.Wait()
	ReportTracerouteStatus(trconfig, vmlist)
}

func processVMTr(vmname string, config *config.TrConfig) {
	vmpath := filepath.Join(config.ResultDir, vmname)
	log.Println("working on", vmpath)
	monvmstatus, err := config.MongoClient.QueryDataStatus(vmname)
	var lastts time.Time

	if len(monvmstatus.TraceFile) > 0 {
		lastts = time.Unix(sptraceroute.ParseTraceFileTs(monvmstatus.TraceFile), 0)
	} else {
		lastts = time.Unix(int64(GlobalStart), 0)
	}
	today := time.Now()
	linkkeymap, faripmap, err := config.MongoClient.CreateLinkmap(convregion(vmname))
	if err != nil {
		config.MMclient.SendPanic(vmname, "failed to create link map", err.Error())
		log.Println("create link map failed", vmname, err)
	}
	for curtime := lastts; curtime.Before(today); curtime = curtime.AddDate(0, 1, 0) {
		monthdir := filepath.Join(vmpath, strconv.Itoa(curtime.Year()), strconv.Itoa(int(curtime.Month())))
		if _, err := os.Stat(monthdir); !os.IsNotExist(err) {
			//month directory exists
			trfiles, err := fileutils.SortResultFiles(monthdir, sptraceroute.ParseTraceFileTs, 0)
			if err != nil {
				log.Println("List result files error", err, monthdir)
			} else {
				monthprefix2as := iputils.NewIPHandlerbyMonth(curtime)
				for _, trfile := range trfiles {
					filets := sptraceroute.ParseTraceFileTs(filepath.Base(trfile))
					if filets > 0 && filets > lastts.Unix() {
						trresult := config.PrepareTraceData(trfile)
						if trresult != nil {
							trresult.Prefix2As = monthprefix2as
							trresult.TraceTs = filets
							sptraceroute.ParseServerTrace(config, trresult, vmname, linkkeymap, faripmap)
							monvmstatus.Mon = vmname
							monvmstatus.TraceFile = filepath.Base(trfile)
							config.MongoClient.UpdateDataStatus(monvmstatus)
							log.Println(vmname, "updated to", filepath.Base(trfile))
						}
					}
				}
			}
		}
	}
}

func ReportTracerouteStatus(config *config.TrConfig, vmlist []string) {
	outstr := []string{"I updated traceroute from these VMs:"}
	for _, vm := range vmlist {
		monstatus, _ := config.MongoClient.QueryDataStatus(vm)
		outstr = append(outstr, monstatus.TraceFile)
	}
	msgstr := strings.Join(outstr, "\n")
	config.MMclient.SendMsg(msgstr, "|")
}

func convregion(vmname string) string {
	vmnamere := regexp.MustCompile(`(\w+-\w+-)\w+`)
	namearr := vmnamere.FindStringSubmatch(vmname)
	if len(namearr) > 0 {
		return namearr[1] + "1"
	}
	return vmname
}
