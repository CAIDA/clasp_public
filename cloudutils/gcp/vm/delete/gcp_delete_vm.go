package main

import (
	"cloudutils/gcp/vm/vmutils"
	"context"
	"flag"
	"spservers/spdb"
)

func main() {
	vmNamePtr := flag.String("v", "None", "required: virtual machine name (must be unique gcp vm name)")
	zonePtr := flag.String("z", "None", "required: the name of zone for this request")
	dbConfigPtr := flag.String("d", "None", "optional: local mongo db config to update local vm info")
	flag.Parse()

	instance := *vmNamePtr
	dbConfig := *dbConfigPtr
	project := "webspeedtest-caida"
	zone := *zonePtr

	// inint service client
	ctx := context.Background()
	id := vmutils.DeleteInstance(ctx, instance, zone, project)

	// update local db
	if dbConfig != "None" {
		db := spdb.InitMongoDB(dbConfig)
		spdb.DeleteVMByID(db, spdb.VMCollection, id)
	}
}
