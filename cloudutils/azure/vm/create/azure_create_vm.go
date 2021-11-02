package main

import (
	"cloudutils/azure/vm/config"
	"cloudutils/azure/vm/vmutils"
	"context"
	"flag"
	"log"
	"spservers/spdb"
)

func main() {
	// parse user arguments
	vmConfigPathPtr := flag.String("v", "None", "required: name of vm to be configured")
	sshPathPtr := flag.String("s", "None", "required: path to ssh public key file")
	dbConfigPtr := flag.String("d", "None", "optional: path to db configuration file")
	flag.Parse()

	vmName := *vmConfigPathPtr
	sshKeyPath := *sshPathPtr
	dbConfig := *dbConfigPtr

	// load default config, requires clientID, userAgent, SubscriptionID, GroupName, Location from environment
	config.Environment()

	// create network interface
	location := config.Location()
	zone := vmutils.GetNextZone(config.GroupName(), location, config.SubscriptionID())
	nicName := vmutils.GenerateNicForVMWithNewIP(vmName, location, zone)

	// create vm
	res, err := vmutils.CreateInstance(context.Background(), vmName, nicName, sshKeyPath, zone)
	vmutils.AzureHandleErr(err)

	log.Println("azure vm created: ", *res.Name)

	// insert into db
	if dbConfig != "None" {
		ip, dns := vmutils.GetIPDNS(context.Background(), res)
		vm := spdb.VMInfo{
			Type:   "azure",
			Name:   *res.Name,
			ID:     *res.ID,
			Status: vmutils.GetPowerStatus(context.Background(), res),
			Zone:   *res.Location + "-" + zone,
			Ipv4:   ip,
			DNS:    dns,
		}
		vms := []spdb.VMInfo{vm}
		db := spdb.InitMongoDB(dbConfig)
		spdb.InsertInstanceIDName(db, vms, spdb.VMCollection)
	}
}
