package sptraceroute

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"os/exec"
	"path/filepath"
	"regexp"
	"serverlinks/config"
	"sort"
	"spservers/spdb"
	"strconv"
	"strings"
	"sync"
)

const (
	Comcast = 1
	Ookla   = 2
	Ndt     = 3
)

type Testplatform int

type ServerLink struct {
	Type       Testplatform
	ServerIP   string
	Lnk        *spdb.Link
	Traceroute *SCTraceroute
}
type ServerLinkwithVP struct {
	VP string
	ServerLink
}

//struct for parsing scamper traceroute. do not parse all the fields here. only extract those required ones.
type SCTraceroute struct {
	Type  string  `json:"type"`
	DstIP string  `json:"dst"`
	DstAS string  `json:"-"`
	Hops  []SCHop `json:"hops"`
}
type SCHop struct {
	Addr     string  `json:"addr"`
	ProbeTTL int     `json:"probe_ttl"`
	RTT      float64 `json:"rtt"`
	AS       string  `json:"-"`
}

//func ParseServerTrace(Param *config.TrConfig, idlink map[string]*bdrmaplink.Link, farlink map[string][]*bdrmaplink.Link, servermap map[string]*ServerLink, prefixip *iputils.IPHandler, platform Testplatform) {
func ParseServerTrace(Param *config.TrConfig, TrResult *config.TrResult, vmname string, linkkeymap map[string]*spdb.Link, faripmap map[string][]*spdb.Link) {
	var out bytes.Buffer
	allwarts := []string{TrResult.ComcastWartsFile, TrResult.NDTWartsFile, TrResult.OoklaWartsFile}
	defer TrResult.CleanupTmp()
	//	alltrs := make([]*spdb.Traceroute, 0)
	log.Printf("Processing traceroute %s %d\n", vmname, TrResult.TraceTs)
	for _, tracewarts := range allwarts {
		tracejsoncmd := exec.Command(filepath.Join(Param.ScamperBin, "sc_warts2json"), tracewarts)
		tracejsoncmd.Stdout = &out
		err := tracejsoncmd.Run()
		if err != nil {
			log.Println(err) //o. he is so scary, fear, and need a panic button  >.<
			Param.MMclient.SendPanic("warts2json error:", err.Error(), tracewarts)
		}
		var trwg sync.WaitGroup
		trresultchan := make(chan *spdb.Traceroute)
		trworkers := make(chan int, Param.TrWorker)
		lines := strings.Split(out.String(), "\n")
		go TracerouteCollector(Param, trresultchan)
		for _, line := range lines {
			var tr SCTraceroute
			if len(line) < 2 {
				continue
			}
			err := json.Unmarshal([]byte(line), &tr)
			if err != nil {
				log.Panic(err)
			}
			if tr.Type == "trace" {
				trwg.Add(1)
				go func() {
					trworkers <- 1
					//traceroute with less than 2 hops, or it is a duplicate server, simply skip ^.^
					if spservers, err := Param.MongoClient.QueryServersbyIPv4(net.ParseIP(tr.DstIP)); len(tr.Hops) >= 2 && len(spservers) > 0 && err == nil {
						dbtrace := spdb.Traceroute{Region: vmname, DstIP: tr.DstIP, DstAS: spservers[0].Asnv4, SpServerId: spservers[0].SpId, Ts: TrResult.TraceTs}
						//sort by hop ttl
						sort.Slice(tr.Hops, func(i, j int) bool { return tr.Hops[i].ProbeTTL < tr.Hops[j].ProbeTTL })
						prevIdx := 0
						dbtrace.Hops = make([]spdb.TrHop, 0)
						for h := 1; h < len(tr.Hops); h++ {
							dbtrace.Hops = append(dbtrace.Hops, spdb.TrHop{Addr: tr.Hops[h].Addr, ProbeTTL: tr.Hops[h].ProbeTTL, Rtt: tr.Hops[h].RTT, Asn: TrResult.Prefix2As.IPv4toASN(net.ParseIP(tr.Hops[h].Addr))})
							//consecutive hops. hops. and bunnies hops again.
							if dbtrace.LinkId.IsZero() {
								if tr.Hops[h].ProbeTTL == (tr.Hops[prevIdx].ProbeTTL + 1) {
									//check for near-far @.@
									key := tr.Hops[prevIdx].Addr + "-" + tr.Hops[h].Addr
									if link, lexist := linkkeymap[key]; lexist {
										//								link, err := Param.MongoClient.QueryLinkbyKey(vmname, key)
										//								log.Println("Linkkey", link.LinkId)
										//								if err != nil {
										//									log.Panic("Query error", err)
										//								}
										if link.Linkkey == key {
											dbtrace.LinkId = link.LinkId
										}
									}
									//found link. !!!! YAY!!!!! ^.^
								}
								if dbtrace.LinkId.IsZero() {
									//								if plinks, err := Param.MongoClient.QueryLinkbyFar(Convertfirstvm(vmname), tr.Hops[h].Addr); err == nil && len(plinks) > 0 {
									if links, lexist := faripmap[tr.Hops[h].Addr]; lexist {
										//possible match by farip
										for _, lnk := range links {
											//direct peering
											if lnk.FarAS == dbtrace.DstAS {
												dbtrace.LinkId = lnk.LinkId
												break
											}
										}
										if dbtrace.LinkId.IsZero() {
											dbtrace.LinkId = links[0].LinkId
										}
									}
								}
								/*							if _, fexist := farlink[tr.Hops[h].Addr]; fexist {
															//only match far address, assign the first one in the slice for now
															if as, found, err := prefixip.GetByString(tr.Hops[h].Addr); err == nil && found {
																tr.Hops[h].AS = as.(string)
															}
														}*/
							}
						}
						trresultchan <- &dbtrace
					}
					<-trworkers
					trwg.Done()
					//	alltrs = append(alltrs, &dbtrace)
				}()
			}
		}
		trwg.Wait()
		close(trresultchan)
	}
	/*
		lentr, err := Param.MongoClient.InsertManyTraceroutes(alltrs)
		if err != nil {
			log.Panic("Insertion error", err)
		}
		log.Printf("%s %d Inserted %d traceroutes\n", vmname, TrResult.TraceTs, lentr)*/
}

func TracerouteCollector(Param *config.TrConfig, trchan chan *spdb.Traceroute) {
	alltrs := make([]*spdb.Traceroute, 0)
	for trelem := range trchan {
		alltrs = append(alltrs, trelem)
	}
	if len(alltrs) > 0 {
		lentr, err := Param.MongoClient.InsertManyTraceroutes(alltrs)
		if err != nil {
			log.Panic("Insertion error", err)
		}
		log.Printf("Inserted %d traceroutes\n", lentr)
	}
}

func Convertfirstvm(vmname string) string {
	vmregx := regexp.MustCompile(`(\w+-\w+-)\w+`)
	names := vmregx.FindStringSubmatch(vmname)
	if len(names) == 0 {
		return vmname
	}
	return names[1] + "1"
}

func ParseTraceFileTs(filename string) int64 {
	trresultre := regexp.MustCompile(`(\w+-\w+-\d+)\.(\d+)\.trace\.tar\.bz2`)
	bname := filepath.Base(filename)
	bnamearr := trresultre.FindStringSubmatch(bname)
	if len(bnamearr) == 0 {
		return 0
	}
	ts, err := strconv.ParseInt(bnamearr[2], 10, 64)
	if err != nil {
		return 0
	} else {
		return ts
	}
}
