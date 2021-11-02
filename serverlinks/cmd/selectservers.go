package main

import (
	"fmt"
	"log"
	"math"
	"serverlinks/config"
	"serverlinks/sptraceroute"
	"sort"
	"spservers/spdb"
	"strconv"
	"sync"
)

func main() {
	var wg, wgres sync.WaitGroup
	SsParam := config.ReadSsConfig()
	allregions, err := SsParam.MongoClient.ListRegions()
	if err != nil {
		log.Panic(err)
	}
	allresults := make([]*config.SsResult, 0)
	reschan := make(chan *config.SsResult)
	workchan := make(chan int, SsParam.Worker)
	wgres.Add(1)
	go func() {
		allresults = SelectSpserverCollector(allresults, reschan)
		wgres.Done()
	}()
	for _, region := range allregions {
		log.Println(region)
		if config.VMNumber(region) == 1 {
			wg.Add(1)
			go func(rg string) {
				workchan <- 1
				sptraceroute.MergeLinkSpservers(SsParam, rg, reschan)
				<-workchan
				wg.Done()
			}(region)
		}
	}
	wg.Wait()
	close(reschan)
	wgres.Wait()
	for _, servers := range allresults {
		log.Println("Region", servers.Region, servers.SpServerId)
	}
	log.Println("Allresult length", len(allresults))
	mmreportdata := UpdateTargets(SsParam, allresults)
	MMRunReport(SsParam, mmreportdata)
	logmap, err := config.OutputServerlist(SsParam.MongoClient, "./")
	if err == nil {
		logstr := []string{"Speedserver assignment updated\n"}
		for name, cnt := range logmap {
			logstr = append(logstr, name+":"+strconv.Itoa(cnt)+"\n")
		}
		SsParam.MMclient.SendInfo(logstr...)
	} else {
		SsParam.MMclient.SendPanic(err.Error())
	}
}

func SelectSpserverCollector(allresults []*config.SsResult, resultch chan *config.SsResult) []*config.SsResult {
	for res := range resultch {
		allresults = append(allresults, res)
	}
	log.Println("Exit result collector", len(allresults))
	return allresults
}

type RunRecord struct {
	SelectedTotal      int
	UnallocatedTargets int
	InsertedTargets    int
	UpdatedTargets     int
}

func UpdateTargets(ssparam *config.SsConfig, allresult []*config.SsResult) map[string]*RunRecord {
	runrec := make(map[string]*RunRecord)
	smeasmap := ssparam.MongoClient.QueryMapSpeedMeas()
	if smeasmap == nil {
		log.Fatal("SpeedMeas map nil")
	}
	servertoupdate := make([]*spdb.SpeedMeas, 0)
	servertoinsert := make([]*spdb.SpeedMeas, 0)
	updatedsmeas := make(map[string]int)
	updatedregions := make(map[string]int)
	//sort targets according to reasons
	sort.Slice(allresult, func(i, j int) bool {
		return allresult[i].Reason < allresult[j].Reason
	})
	for _, result := range allresult {
		//		spserver, errs := ssparam.MongoClient.QueryServerbyId(result.SpServerId)
		//		link,errl := ssparam.MongoClient.QueryLinkbyId(result.LinkId)
		//		if errs == nil && errl == nil {
		skey := config.VMNametoRegion(result.Region) + ":" + result.SpServerId.Hex()
		updatedregions[config.VMNametoRegion(result.Region)] = 1
		updatedsmeas[skey] = 1
		log.Println("skey", skey)
		if smeas, sexist := smeasmap[skey]; sexist {
			if !smeas[0].Enabled {
				//this server is currently disabled
				smeasmap[skey][0].Enabled = true
				smeasmap[skey][0].Mon = config.VMNametoRegion(result.Region) + "-0"
				smeasmap[skey][0].Reason = result.Reason
				smeasmap[skey][0].Activeperiod = append(smeasmap[skey][0].Activeperiod, spdb.TimePeriod{Start: ssparam.EnableDate})
				servertoupdate = append(servertoupdate, smeasmap[skey][0])
			} else {
				log.Println("Existing server", skey, smeasmap[skey][0].Mon)
			}
			//existing duplicate target
			if len(smeasmap[skey]) > 1 {
				log.Println("Duplicate target", skey)
				for s := 1; s < len(smeasmap[skey]); s++ {
					log.Println("  ", smeasmap[skey][s].Mon, smeasmap[skey][s].Link)
					smeasmap[skey][s].Enabled = false
					smeasmap[skey][s].Reason = -99
					smeasmap[skey][s].Activeperiod[len(smeasmap[skey][s].Activeperiod)-1].End = ssparam.EnableDate
					servertoupdate = append(servertoupdate, smeasmap[skey][s])
				}
			}
		} else {
			//add new server, create a record
			actper := []spdb.TimePeriod{spdb.TimePeriod{Start: ssparam.EnableDate}}
			//temporarily add to VM "0"
			newserver := &spdb.SpeedMeas{Mon: config.VMNametoRegion(result.Region) + "-0", SpeedServer: result.SpServerId, Enabled: true, Link: result.LinkId, Activeperiod: actper, Assigntype: "auto", Reason: result.Reason}
			servertoinsert = append(servertoinsert, newserver)
			smeasmap[skey] = []*spdb.SpeedMeas{newserver}
			log.Println("Add new server", newserver)
		}
	}
	//Disable all other "auto" (this will keep other types)
	for smeaskey, smeas := range smeasmap {
		if _, rexist := updatedregions[config.VMNametoRegion(smeas[0].Mon)]; rexist {
			if _, uexist := updatedsmeas[smeaskey]; !uexist {
				for s := 0; s < len(smeas); s++ {
					if smeas[s].Assigntype == "auto" && smeas[s].Enabled {
						log.Println("Disabling target", smeasmap[smeaskey][s], smeasmap[smeaskey][s].Mon)
						smeasmap[smeaskey][s].Enabled = false
						smeasmap[smeaskey][s].Reason = -99
						smeasmap[smeaskey][s].Activeperiod[len(smeasmap[smeaskey][s].Activeperiod)-1].End = ssparam.EnableDate
						servertoupdate = append(servertoupdate, smeasmap[smeaskey][s])
					}
				}
			}
		}
	}
	//count targets in each region.
	regioncnt := make(map[string]int)
	regionkeys := make(map[string][]string)
	for smeaskey, smeas := range smeasmap {
		//we still iterate over the slice, but expected that index >0 has been disabled
		for s := 0; s < len(smeas); s++ {
			if _, uexist := updatedregions[config.VMNametoRegion(smeas[s].Mon)]; uexist {
				runrec[config.VMNametoRegion(smeas[s].Mon)] = &RunRecord{}
				if smeas[s].Enabled {
					if _, rexist := regioncnt[config.VMNametoRegion(smeas[s].Mon)]; !rexist {
						regioncnt[config.VMNametoRegion(smeas[s].Mon)] = 1
						regionkeys[config.VMNametoRegion(smeas[s].Mon)] = []string{smeaskey}
					} else {
						regioncnt[config.VMNametoRegion(smeas[s].Mon)] = regioncnt[config.VMNametoRegion(smeas[s].Mon)] + 1
						regionkeys[config.VMNametoRegion(smeas[s].Mon)] = append(regionkeys[config.VMNametoRegion(smeas[s].Mon)], smeaskey)
					}
				}
			}
		}
	}
	vmtoset := make([]string, 0)
	//iterate over all regions, new targets are put into 0 by default
	for regionkey, regioncount := range regioncnt {
		//compute the VMs needed in each region
		numvm := int(math.Ceil(float64(regioncount) / float64(ssparam.TargetperVM)))
		log.Println("Region", regionkey, "needs", numvm, "VMs")
		runrec[regionkey].SelectedTotal = regioncount
		if numvm > ssparam.MaxVMperRegion {
			numvm = ssparam.MaxVMperRegion
			log.Println("Region", regionkey, "will construct", numvm, "VMs")
		}
		//loop over the VMs and move extra VMs to 0
		vmmap := make(map[string][]string)
		for _, vmkeys := range regionkeys[regionkey] {
			vmnum := config.VMNumber(smeasmap[vmkeys][0].Mon)
			if vmnum == 0 {
				vmtoset = append(vmtoset, vmkeys)
			} else {
				if vmnum > numvm {
					log.Println("Target in closing VM", vmkeys)
					//current vm number is larger than the vm to be used, set it to 0, and redistribute it later
					smeasmap[vmkeys][0].Mon = config.VMNametoRegion(smeasmap[vmkeys][0].Mon) + "-0"
					servertoupdate = append(servertoupdate, smeasmap[vmkeys][0])
					vmtoset = append(vmtoset, vmkeys)
				} else {
					log.Println("Check Target", vmkeys, smeasmap[vmkeys][0].Mon, smeasmap[vmkeys][0].Enabled)
					if smeasmap[vmkeys][0].Enabled {
						if _, vexist := vmmap[smeasmap[vmkeys][0].Mon]; !vexist {
							vmmap[smeasmap[vmkeys][0].Mon] = []string{vmkeys}
						} else {
							vmmap[smeasmap[vmkeys][0].Mon] = append(vmmap[smeasmap[vmkeys][0].Mon], vmkeys)
						}
					}
				}
			}
		}
		//redistribute VMs
		for v := 1; v <= numvm; v++ {
			vmnames := regionkey + "-" + strconv.Itoa(v)
			vmroom := 0
			if vms, vexist := vmmap[vmnames]; vexist {
				vmroom = ssparam.TargetperVM - len(vms)
			} else {
				vmroom = ssparam.TargetperVM
			}
			if vmroom > 0 {
				log.Println("VM", vmnames, "has", len(vmmap[vmnames]), "and has room", vmroom, "and has remaining", len(vmtoset))
				log.Println("  ", vmmap[vmnames])
				setvmidx := 0
				if len(vmtoset) > vmroom {
					//this vm is not going to fit all new ones
					setvmidx = vmroom
				} else {
					//this vm can fit all targets
					setvmidx = len(vmtoset)
				}
				//set the first setvmidx targets as this monitor
				for _, target := range vmtoset[:setvmidx] {
					smeasmap[target][0].Mon = vmnames
					log.Println("Target", target, "assigned to", vmnames)
				}
				//cut them out from vmtoset, this slice will be empty if all targets are allocated
				vmtoset = vmtoset[setvmidx:]
			} else if vmroom == 0 {
				log.Println("VM", vmnames, "is full")
			} else {
				//this vm is overflow
				log.Println("VM", vmnames, "currently overflow")
				overflown := len(vmmap[vmnames]) - ssparam.TargetperVM
				extravms := vmmap[vmnames][overflown:]
				vmmap[vmnames] = vmmap[vmnames][:overflown]
				for _, vmkeys := range extravms {
					smeasmap[vmkeys][0].Mon = config.VMNametoRegion(smeasmap[vmkeys][0].Mon) + "-0"
					vmtoset = append(vmtoset, vmkeys)
					log.Println("Overflown Target", vmkeys, "removed from ", vmnames)
				}
			}
		}
		if len(vmtoset) > 0 {
			//we need more VMs than existing ones
			runrec[regionkey].UnallocatedTargets = len(vmtoset)
			log.Println("Still have", len(vmtoset), "targets cannot be allocated", vmtoset)
			vmtoset = []string{}
		}
	}
	//	finalupdate := make([]*spdb.SpeedMeas, 0)
	finalinsert := make([]*spdb.SpeedMeas, 0)
	for _, server := range servertoinsert {
		if config.VMNumber(server.Mon) > 0 {
			log.Println(server)
			runrec[config.VMNametoRegion(server.Mon)].InsertedTargets += 1
			finalinsert = append(finalinsert, server)
		} else {
			log.Println("Not adding", server)
		}
	}
	//commit to db
	_, err := ssparam.MongoClient.InsertManySpeedMeas(finalinsert)
	if err != nil {
		log.Println("Insert error", err)
		ssparam.MMclient.SendPanic("Insert target error", err.Error())
	}
	log.Println("Final to update")
	for _, server := range servertoupdate {
		if (config.VMNumber(server.Mon) > 0 && server.Enabled) || server.Enabled == false {
			log.Println(server)
			err := ssparam.MongoClient.UpdateSpeedserver(server)
			if err != nil {
				log.Println("Update error", err)
				ssparam.MMclient.SendPanic("Update target error", err.Error())
			}
			runrec[config.VMNametoRegion(server.Mon)].UpdatedTargets += 1
			//finalupdate = append(finalupdate, server)
		} else {
			log.Println("Dropped update plan", server)
		}
	}
	return runrec
}

func MMRunReport(ssparam *config.SsConfig, runreportdata map[string]*RunRecord) {
	runreport := []string{"Select Target report:\n"}
	for region, stat := range runreportdata {
		runreport = append(runreport, fmt.Sprintf(" %s: ,Total: %d, Discarded: %d, Updated: %d, Inserted: %d\n", region, stat.SelectedTotal, stat.UnallocatedTargets, stat.UpdatedTargets, stat.InsertedTargets))
	}
	ssparam.MMclient.SendInfo(runreport...)
}
