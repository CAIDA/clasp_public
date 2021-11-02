package bdrmaplink

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"serverlinks/config"
	"spservers/spdb"
	"strconv"
	"strings"
)

func GenerateLinks(GlbParam *config.BdrConfig, Param *config.BdrResult, linkmap map[string]*spdb.Link, faripmap map[string][]*spdb.Link) {
	log.Println("GenerateRouterFile start")
	if len(Param.LinkFile) == 0 {
		scbdrmapcmd := exec.Command(filepath.Join(GlbParam.ScamperBin, "sc_bdrmap"), "-d", "routers", "-a", Param.Prefix2ASFile, "-g", Param.DelegationFile, "-r", Param.ASRelFile, "-v", Param.SiblingFile, "-x", Param.PeeringFile, Param.BdrWartsFile)
		log.Println("Wart file", Param.BdrWartsFile)
		log.Println(scbdrmapcmd)
		var out bytes.Buffer
		scbdrmapcmd.Stdout = &out

		err := scbdrmapcmd.Run()
		if err != nil {
			log.Panic(err)
		}
		// convert from /results/bdrmap/xxx.warts to tmpdir/xxx.router.txt
		Param.RTRFile = filepath.Join(Param.Tmpdir, filepath.Base(Param.BdrWartsFile[:len(Param.BdrWartsFile)-len(filepath.Ext(Param.BdrWartsFile))])) + ".router.txt"

		f, err := os.Create(Param.RTRFile)
		if err != nil {
			log.Panic(err)
		}
		_, err = f.Write(out.Bytes())
		if err != nil {
			log.Panic(err)
		}
		f.Sync()
		log.Println("Writing Router file", Param.RTRFile)
		defer f.Close()

		//run the script to extract links and aliases
		getlnkcmd := exec.Command("python", config.GETLINKSCRIPT, "-w", Param.BdrWartsFile, "-b", Param.RTRFile, "-s", Param.SiblingFile)
		log.Println(getlnkcmd)
		err = getlnkcmd.Run()
		if err != nil {
			log.Panic(err)
		}
		Param.LinkFile = filepath.Join(Param.Tmpdir, filepath.Base(Param.BdrWartsFile[:len(Param.BdrWartsFile)-len(filepath.Ext(Param.BdrWartsFile))])) + ".links.out"
		Param.AliasesFile = filepath.Join(Param.Tmpdir, filepath.Base(Param.BdrWartsFile[:len(Param.BdrWartsFile)-len(filepath.Ext(Param.BdrWartsFile))])) + ".aliases.out"
	} else {
		log.Println("Use previous link file:", Param.LinkFile)
	}
	GenerateLinkmap(Param.LinkFile, linkmap, faripmap)
	log.Println("Get link completed")
}

func GenerateLinkmap(LinkFile string, linkmap map[string]*spdb.Link, faripmap map[string][]*spdb.Link) {
	linkfile, err := os.Open(LinkFile)
	if err != nil {
		log.Panic(err)
	}
	defer linkfile.Close()

	scanner := bufio.NewScanner(linkfile)
	for scanner.Scan() {
		tmplnk := strings.Split(scanner.Text(), "|")
		if len(tmplnk) == 7 {
			lkey := tmplnk[0] + "-" + tmplnk[1]
			if _, exist := linkmap[lkey]; !exist {
				linkmap[lkey] = &spdb.Link{NearIP: tmplnk[0], FarIP: tmplnk[1], FarAS: tmplnk[2], Covered: false}
			}
		}
	}
	for _, linkobj := range linkmap {
		if _, exist := faripmap[linkobj.FarIP]; !exist {
			faripmap[linkobj.FarIP] = []*spdb.Link{linkobj}
		} else {
			seenas := false
			for _, flink := range faripmap[linkobj.FarIP] {
				if linkobj.FarAS == flink.FarAS {
					seenas = true
					break
				}
			}
			if !seenas {
				faripmap[linkobj.FarIP] = append(faripmap[linkobj.FarIP], linkobj)
			}
			//		log.Println("Has more than one link have the same far ip:", linkkey, linkobj.FarIP, len(faripmap[linkobj.FarIP]))
		}
	}
	for _, farobj := range faripmap {
		if len(farobj) > 1 {
			fmt.Println("FarIP:", farobj[0].FarIP)
			fmt.Println("  - ")
			for _, link := range farobj {
				fmt.Printf(link.FarAS + " ")
			}
		}
	}
	//i love my bun
}

func ParseBdrmapFileTs(filename string) int64 {
	bdrresultre := regexp.MustCompile(`(\w+-\w+-\d+)\.(\d+)\.tar\.bz2`)
	//use Base to extract the filename (in case input is absolute path)
	filenamebase := filepath.Base(filename)
	filearr := bdrresultre.FindStringSubmatch(filenamebase)
	//name pattern does not match
	if len(filearr) == 0 {
		return 0
	}
	ts, err := strconv.ParseInt(filearr[2], 10, 64)
	if err != nil {
		return 0
	} else {
		return ts
	}
}
