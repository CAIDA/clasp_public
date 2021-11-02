package sptraceroute

import (
	"log"
	"math"
	"serverlinks/config"
	"sort"
	"spservers/spdb"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"gonum.org/v1/gonum/stat"
)

//implements the logic for selection speedtest servers
func MergeLinkSpservers(ssparam *config.SsConfig, region string, resultch chan *config.SsResult) {
	log.Println("working on", region)
	reschan := make(chan *spdb.LinkSpAgg)
	go func() {
		err := ssparam.MongoClient.QueryLinksSpServerMatch(region, ssparam.StartDate.Unix(), reschan)
		if err != nil {
			log.Println(err)
		}
		close(reschan)
	}()
	serverrec := make(map[string]int)
	for res := range reschan {
		selectedspidx := ""
		done := false
		reason := -99
		rtt := 0.0
		if len(res.LinkObj) == 0 {
			log.Println("No link", res)
			continue
		}
		if len(res.SpServerIds) > 0 {
			/*for _, spid := range res.SpServerIds {
				curserver, err := ssparam.MongoClient.QuerySpeedserverExist(region, spid)
				if err == nil {
					if curserver == 1 {
						//this server is current performing measurement, just keep it
						selectedspidx = spid.Hex()
						done = true
						reason = 1
						break
					}
				}
			}*/
			rttmap, tridmap, err := ssparam.MongoClient.TracerouteDestRtt(res.TrIds)
			if err != nil {
				log.Println("compute rtt error", err)
			}
			log.Println("Link:", res.LinkObj[0].Linkkey, res.LinkObj[0].LinkId)
			minrttset := make([]float64, 0)

			for spid, _ := range rttmap {
				sort.Float64s(rttmap[spid])
				if len(rttmap[spid]) > 0 {
					log.Println(" server", spid, "min rtt", rttmap[spid][0])
					minrttset = append(minrttset, rttmap[spid][0])
				}
				//linkopt, _ := ssparam.MongoClient.SpServersLinkChoice(region, spidhex)
				//log.Println(" mean rtt:", spidhex, stat.Mean(rtts, nil), linkopt)
			}
			if len(minrttset) == 0 {
				log.Println("No rtt found")
				continue
			}
			//compute the 10th percentil of RTT
			sort.Float64s(minrttset)
			low10qrtt := stat.Quantile(0.25, 1, minrttset, nil)
			log.Println(" lowq rtt", low10qrtt)
			candspservers := make([]string, 0)
			for spidhex, _ := range rttmap {
				if len(rttmap[spidhex]) > 0 {
					if rttmap[spidhex][0] <= low10qrtt && rttmap[spidhex][0] < ssparam.RttThreshold {
						candspservers = append(candspservers, spidhex)
					}
				}
			}
			//check if any server is in Far AS
			for _, spidhex := range candspservers {
				spid, _ := primitive.ObjectIDFromHex(spidhex)
				spinfo, err := ssparam.MongoClient.QueryServerbyId(spid)
				if err != nil {
					log.Println("query server error", spidhex)
					continue
				}
				if _, srexist := serverrec[spidhex]; !srexist && spinfo.Asnv4 == res.LinkObj[0].FarAS {
					done = true
					selectedspidx = spidhex
					log.Println(" sp is direct peer", spidhex)
					reason = 1
					rtt = rttmap[spidhex][0]
					serverrec[spidhex] = 1
					break
				}
			}
			if !done {
				//all servers are not in Far AS, check the length of AS path
				aspathmap := make(map[string]int)
				trfreqmap := make(map[string]int)
				minlen := 99
				maxfreq := 0
				for _, spidhex := range candspservers {
					aslen, err := ssparam.MongoClient.TracerouteASPathLen(tridmap[spidhex])
					if err != nil {
						log.Println("as path len error")
						continue
					}
					sort.Ints(aslen)
					log.Println("  sp", spidhex, "aslen", aslen)
					trfreqmap[spidhex] = len(aslen)
					if len(aslen) > maxfreq {
						maxfreq = len(aslen)
					}
					for _, asl := range aslen {
						if asl > 0 {
							if _, asexist := aspathmap[spidhex]; !asexist {
								aspathmap[spidhex] = asl
							}
							if asl < minlen {
								minlen = asl
							}
						}
					}
				}
				if maxfreq < ssparam.MinTrThreshold {
					//very few traceroute saw this interconnect.
					done = true
					selectedspidx = ""
					reason = -1
					log.Println("  all servers seldom used this interconnects")
				} else {
					minaspathserver := []string{}
					for spidx, aspath := range aspathmap {
						if aspath == minlen && trfreqmap[spidx] > 10 {
							minaspathserver = append(minaspathserver, spidx)
						}
					}
					if len(minaspathserver) == 0 {
						//targets with shortest AS path had very few freq using that interconnect
						done = true
						selectedspidx = ""
						reason = -2
						log.Println("  no short AS path server available")
					} else {
						mrtt := math.Inf(1)
						mserver := ""
						//pick the one with min rtt or existing
						for _, mser := range minaspathserver {
							tmpspid, _ := primitive.ObjectIDFromHex(mser)
							curserver, err := ssparam.MongoClient.QuerySpeedserverExist(region, tmpspid)
							if err == nil {
								//not a server that we selected before
								if _, srexist := serverrec[mser]; !srexist {
									if curserver == 1 {
										//this server is current performing measurement, just keep it
										mserver = mser
										reason = 3
										break
									} else {
										if len(rttmap[mser]) >= 1 {
											if rttmap[mser][0] < mrtt {
												mserver = mser
												mrtt = rttmap[mser][0]
											}
										}

									}
								}
							}
						}
						if mserver != "" {
							selectedspidx = mserver
							reason = 4
							rtt = mrtt
							done = true
						}
					}
				}
			}
		}
		if done && selectedspidx != "" {
			sspidx, _ := primitive.ObjectIDFromHex(selectedspidx)
			spserver, _ := ssparam.MongoClient.QueryServerbyId(sspidx)
			log.Println("Selected ", spserver.Host, selectedspidx, "for link", res.LinkObj[0].Linkkey, reason)
			lnk := &config.SsResult{Region: region, LinkId: res.LinkObj[0].LinkId, SpServerId: sspidx, Reason: reason, AvgRtt: rtt}
			resultch <- lnk
		} else {
			log.Println("No server selected for link", res.LinkObj[0].Linkkey, reason)
		}
	}
}
