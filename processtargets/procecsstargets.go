package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"spservers/spdb"
	"strings"
)

type Vmcount struct {
	Same int
	Prem int
	Std  int
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: processtargets <input csv> <mongodb config>")
	}
	inputcsv := os.Args[1]
	mongodbcfg := os.Args[2]
	csvfile, err := os.Open(inputcsv)
	if err != nil {
		log.Fatal(err)
	}
	defer csvfile.Close()

	vmzonemap := make(map[string][]string)
	vmzonemap["US-Central1-a"] = []string{"gcp-central1-10"}
	vmzonemap["US-East1-b"] = []string{"gcp-east1-10"}
	vmzonemap["Asia-Northeast1-b"] = []string{"gcp-asianortheast1-1"}
	vmzonemap["Europe-West1-b"] = []string{"gcp-euwest1-1"}

	vmcntmap := make(map[string]*Vmcount)
	vmcntmap["US-Central1-a"] = &Vmcount{0, 0, 0}
	vmcntmap["US-East1-b"] = &Vmcount{0, 0, 0}
	vmcntmap["Asia-Northeast1-b"] = &Vmcount{0, 0, 0}
	vmcntmap["Europe-West1-b"] = &Vmcount{0, 0, 0}

	mdb := spdb.NewMongoDB(mongodbcfg, "speedtest")
	if mdb == nil {
		log.Fatal("Connect to mongodb failed")
	}
	defer mdb.Close()
	scanner := bufio.NewScanner(csvfile)
	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Split(line, ",")
		if sip := net.ParseIP(data[1]); sip != nil {
			spservers, err := mdb.QueryServersbyIPv4(sip)
			if err != nil {
				log.Fatal("db error", err)
			}
			//should only have 1 server. otherwise, we still only take the first one
			if len(spservers) >= 1 {
				printout := true
				switch data[2] {
				case "about_the_same":
					vmcntmap[data[0]].Same++
					if vmcntmap[data[0]].Same > 6 {
						printout = false
					}
				case "premium_better":
					vmcntmap[data[0]].Prem++
					if vmcntmap[data[0]].Prem > 6 {
						printout = false
					}
				case "standard_better":
					vmcntmap[data[0]].Std++
					if vmcntmap[data[0]].Std > 6 {
						printout = false
					}
				}
				if printout {
					fmt.Println(strings.Join([]string{vmzonemap[data[0]][0], spservers[0].IPv4, spservers[0].Asnv4, spservers[0].Type, spservers[0].Identifier}, "|"))
				}
			} else {
				log.Println("server not found", sip.String())
			}
		}
	}

}
