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
	// parse arguments
	opPtr := flag.String("o", "None", "required: start/stop")
	vmNamePtr := flag.String("v", "None", "required: virtual machine name (must be unique azure name)")
	dbConfigPtr := flag.String("d", "None", "optional: local mongo db config to update local vm info")
	flag.Parse()

	op := *opPtr
	vmName := *vmNamePtr
	dbConfig := *dbConfigPtr

	// do operation
	if op != "start" && op != "stop" {
		log.Println("Please provide start/stop as the operation command")
	}

	// load config, requires clientID, userAgent, SubscriptionID, GroupName
	config.Environment()

	// start/stop instances
	ctx := context.Background()
	var pStatus string
	switch op {
	case "start":
		log.Println(vmutils.StartInstance(ctx, vmName))
		pStatus = "running"
	case "stop":
		log.Println((vmutils.StopInstance(ctx, vmName)))
		pStatus = "stopped"
	}

	resID := *vmutils.GetVM(ctx, vmutils.GetVMClient(), vmName).ID
	if dbConfig != "None" {
		db := spdb.InitMongoDB(dbConfig)
		spdb.UpdateState(db, "vmInfo", pStatus, resID)
	}
}
