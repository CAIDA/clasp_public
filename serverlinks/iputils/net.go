package iputils

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/zmap/go-iptree/iptree"
)

const (
	Prefix2ASv4Latest = "/data/routing/routeviews-prefix2as/routeviews-rv2-latest.pfx2as.gz"
	Prefix2ASv6Latest = "/data/routing/routeviews6-prefix2as/routeviews-rv6-latest.pfx2as.gz"
	Routeviewv4dir    = "/data/routing/routeviews-prefix2as/%d/%0.2d/"
	Routeviewv4name   = "routeviews-rv2-%d%0.2d%0.2d-*.pfx2as.gz"
)

type IPHandler interface {
	ResolveV4(host string) (string, string)
	IPv4toASN(ip net.IP) string
}

type ipHandler struct {
	Treev4 *iptree.IPTree
	Treev6 *iptree.IPTree
}

func NewIPHandler(options ...string) IPHandler {
	h := new(ipHandler)
	log.Println("New iphandler")
	if len(options) == 0 {
		tmpv4, err := ioutil.TempFile("", "pfx2as")
		if err != nil {
			log.Fatal("Create tmp pfx2as error", err)
		}
		defer os.Remove(tmpv4.Name())
		out, err := exec.Command("gzcat", Prefix2ASv4Latest).Output()
		if err != nil {
			if _, err = tmpv4.Write(out); err != nil {
				log.Fatal("Write to tmpfile error", err)
			}
			if err = tmpv4.Close(); err != nil {
				log.Fatal(err)
			}
			h.Treev4 = prepareIP2ASTrie(tmpv4.Name())
			log.Println("Built Trie", tmpv4.Name())
		} else {
			log.Fatal("Obtain latest pfx2as error", err)
		}
	} else {
		if len(options) == 1 {
			h.Treev4 = prepareIP2ASTrie(options[0])
		}
	}
	return h
}

//new iphandler of thte first day of the month of ts
//if data cannot be found, simple return latest
func NewIPHandlerbyMonth(ts time.Time) IPHandler {
	if !ts.IsZero() {
		monthdir := fmt.Sprintf(Routeviewv4dir, ts.Year(), int(ts.Month()))
		filenamewild := fmt.Sprintf(Routeviewv4name, ts.Year(), int(ts.Month()), 1)
		matchfile, err := filepath.Glob(filepath.Join(monthdir, filenamewild))
		if err != nil {
			log.Panic(err, filepath.Join(monthdir, filenamewild))
		}
		//assume there is only one file match
		if len(matchfile) > 0 {
			return NewIPHandler(matchfile[0])
		} else {
			return NewIPHandler()
		}
	}
	return NewIPHandler()
}

func (i *ipHandler) IPv4toASN(ip net.IP) string {
	if i.Treev4 != nil && ip != nil {
		//search prefix2as trie
		if asnval, foundasn, err := i.Treev4.GetByString(ip.String()); err == nil && foundasn {
			return strings.TrimSpace(asnval.(string))
		} else {
			//cannot find a record in prefix2as, try cymru
			tmpip := ip.To4()
			asnq := net.IPv4(tmpip[3], tmpip[2], tmpip[1], tmpip[0]).String() + ".origin.asn.cymru.com"
			outtxt, err := net.LookupTXT(asnq)
			if err != nil {
				return ""
			}
			asnstr := strings.Split(outtxt[0], "|")
			return strings.TrimSpace(asnstr[0])
		}
	} else {
		log.Fatal("IPv4 Tree is nil")
	}
	return ""

}

//input: hostname
//output: ipv4, asnv4
func (i *ipHandler) ResolveV4(host string) (string, string) {
	addrs := strings.Split(host, ":")
	if len(host) > 0 {
		ips, err := net.LookupIP(addrs[0])
		if err != nil {
			log.Println(err)
			return "", ""
		}
		if len(ips) > 0 {
			//log.Println(addrs[0], ips)
			j := 0
			foundv4 := -1
			for j = 0; j < len(ips); j++ {
				if ips[j].To4() != nil {
					foundv4 = j
					break
				}
			}
			if foundv4 < 0 {
				log.Println("No IPv4 found")
				return "", ""
			}
			return ips[foundv4].String(), i.IPv4toASN(ips[foundv4])
		}
	}
	return "", ""

}

//given a hostname:port, return the first ipv4 address, and the asn
/*func ResolveNet(host string) (string, string) {
	addrs := strings.Split(host, ":")
	if len(host) > 0 {
		ips, err := net.LookupIP(addrs[0])
		if err != nil {
			log.Println(err)
			return "", ""
		}
		if len(ips) > 0 {
			//log.Println(addrs[0], ips)
			i := 0
			foundv4 := -1
			for i = 0; i < len(ips); i++ {
				if ips[i].To4() != nil {
					foundv4 = i
					break
				}
			}
			if foundv4 < 0 {
				log.Println("No IPv4 found")
				return "", ""
			}
			tmpip := ips[foundv4].To4()
			asnq := net.IPv4(tmpip[3], tmpip[2], tmpip[1], tmpip[0]).String() + ".origin.asn.cymru.com"
			outtxt, err := net.LookupTXT(asnq)
			if err != nil {
				return ips[foundv4].String(), ""
			}
			asnstr := strings.Split(outtxt[0], "|")
			return ips[foundv4].String(), strings.TrimSpace(asnstr[0])
		}
	}
	return "", ""
}
*/
//struct for parsing scamper traceroute. do not parse all the fields here. only extract those required ones.
func prepareIP2ASTrie(prefix2asfile string) *iptree.IPTree {
	t := iptree.New()
	ipasnfile, err := os.Open(prefix2asfile)
	if err != nil {
		log.Panic(err)
	}
	scanner := bufio.NewScanner(ipasnfile)
	for scanner.Scan() {
		line := strings.Trim(scanner.Text(), "\n")
		if len(line) > 1 {
			//not comment
			if line[0] != '#' {
				data := strings.Fields(line)
				//expect: 115.146.123.131 32      45903
				if len(data) == 3 {
					t.AddByString(data[0]+"/"+data[1], data[2])
				}
			}
		}
	}
	return t

}
