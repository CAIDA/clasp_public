package config

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"spservers/spdb"
	"strconv"
	"strings"
)

func VMNametoRegion(vmname string) string {
	vmregx := regexp.MustCompile(`(\w+-\w+)-\w+`)
	vmarr := vmregx.FindStringSubmatch(vmname)
	if vmarr != nil {
		return vmarr[1]
	}
	return ""
}

func VMNametoProvider(vmname string) string {
	vmprovregx := regexp.MustCompile(`(\w+)-\w+-\w+`)
	vmarr := vmprovregx.FindStringSubmatch(vmname)
	if len(vmarr) > 1 {
		return vmarr[1]
	}
	return ""
}

func VMNumber(vmname string) int {
	vmnumregx := regexp.MustCompile(`\w+-\w+-(\w+)`)
	vmarr := vmnumregx.FindStringSubmatch(vmname)
	if vmarr != nil {
		vmnum, err := strconv.Atoi(vmarr[1])
		if err != nil {
			return -1
		}
		return vmnum
	}
	return -1
}

func OutputServerlist(mgoclient *spdb.SpeedtestMongo, outputdir string) (map[string]int, error) {
	if mgoclient != nil {
		if allmeasagg := mgoclient.QueryAllEnabledSpeedMeas(); allmeasagg != nil {
			googlefout, err := os.Create(filepath.Join(outputdir, "google-serverlist.txt"))
			if err != nil {
				log.Fatal(err)
			}
			defer googlefout.Close()
			azurefout, err := os.Create(filepath.Join(outputdir, "azure-serverlist.txt"))
			if err != nil {
				log.Fatal(err)
			}
			defer azurefout.Close()
			awsfout, err := os.Create(filepath.Join(outputdir, "amazon-serverlist.txt"))
			if err != nil {
				log.Fatal(err)
			}
			defer awsfout.Close()
			logmap := make(map[string]int)
			for _, speedmeasagg := range allmeasagg {
				if len(speedmeasagg.SpserverInfo) > 0 {
					farip := speedmeasagg.SpserverInfo[0].IPv4
					faras := speedmeasagg.SpserverInfo[0].Asnv4
					meastype := speedmeasagg.SpserverInfo[0].Type
					if meastype == "mlab" {
						meastype = "ndt"
					}
					if len(speedmeasagg.LinkInfo) > 0 {
						if !speedmeasagg.LinkInfo[0].LinkId.IsZero() {
							farip = speedmeasagg.LinkInfo[0].FarIP
							faras = speedmeasagg.LinkInfo[0].FarAS
						}
					}
					if _, lexist := logmap[speedmeasagg.Mon]; !lexist {
						logmap[speedmeasagg.Mon] = 1
					} else {
						logmap[speedmeasagg.Mon] = logmap[speedmeasagg.Mon] + 1
					}
					resultstr := strings.Join([]string{speedmeasagg.Mon, farip, faras, meastype, speedmeasagg.SpserverInfo[0].Identifier}, "|")
					log.Println(resultstr)
					switch VMNametoProvider(speedmeasagg.Mon) {
					case "gcp":
						_, _ = googlefout.WriteString(resultstr + "\n")
					case "ms":
						_, _ = azurefout.WriteString(resultstr + "\n")
					case "aws":
						_, _ = awsfout.WriteString(resultstr + "\n")
					}
				}
			}
			googlefout.Sync()
			azurefout.Sync()
			awsfout.Sync()
			return logmap, nil
		}
		return nil, errors.New("Failed to query enabled speedmes")
	}
	return nil, errors.New("Database is nil")

}
