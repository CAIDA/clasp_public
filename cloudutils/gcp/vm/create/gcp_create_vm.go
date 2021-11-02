package main

import (
	"cloudutils/gcp/vm/vmutils"
	"context"
	"flag"
	"os"
	"spservers/spdb"

	"google.golang.org/api/compute/v1"
)

func main() {
	vmNamePtr := flag.String("v", "None", "required: virtual machine name (must be unique gcp vm name)")
	dbConfigPtr := flag.String("d", "None", "optional: local mongo db config to update local vm info")
	flag.Parse()

	// prepare parameters
	vmName := *vmNamePtr
	dbConfig := *dbConfigPtr
	project := "webspeedtest-caida"

	// launch instance
	ctx := context.Background()
	zone := vmutils.GetNextAvailableZone(ctx, project, os.Getenv("CLOUDSDK_COMPUTE_REGION"))
	vmutils.CreateInstance(ctx, project, zone, vmName)

	// insert into db
	if dbConfig != "None" {
		instance := vmutils.GetInstance(ctx, project, zone, vmName)
		instances := []*compute.Instance{instance}
		vms := vmutils.InstanceToVMInfo(instances)
		db := spdb.InitMongoDB(dbConfig)
		spdb.InsertInstanceIDName(db, vms, spdb.VMCollection)
	}
}
