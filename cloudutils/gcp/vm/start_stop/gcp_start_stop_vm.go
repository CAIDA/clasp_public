package main

import (
	"cloudutils/gcp/vm/vmutils"
	"context"
	"flag"
	"spservers/spdb"
)

func main() {
	// parse flags
	opPtr := flag.String("o", "None", "required: start/stop")
	vmNamePtr := flag.String("v", "None", "required: virtual machine name (must be unique gcp vm name)")
	zonePtr := flag.String("z", "None", "required: the name of zone for this request")
	dbConfigPtr := flag.String("d", "None", "optional: local mongo db config to update local vm info")
	flag.Parse()

	op := *opPtr
	instance := *vmNamePtr
	dbConfig := *dbConfigPtr
	project := "webspeedtest-caida"
	zone := *zonePtr

	// inint service client
	ctx := context.Background()

	// operation
	var pStatus string
	var id string
	switch op {
	case "start":
		id = vmutils.StartInstance(ctx, instance, zone, project)
		pStatus = "running"
	case "stop":
		id = vmutils.StopInstance(ctx, instance, zone, project)
		pStatus = "stopped"
	}

	// update local db
	if dbConfig != "None" {
		db := spdb.InitMongoDB(dbConfig)
		spdb.UpdateState(db, "vmInfo", pStatus, id)
	}
}
