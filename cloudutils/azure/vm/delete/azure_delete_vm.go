package main

import (
	"cloudutils/azure/vm/config"
	"cloudutils/azure/vm/vmutils"
	"context"
	"flag"
	"spservers/spdb"
)

func main() {
	// parse arguments
	vmNamePtr := flag.String("v", "None", "required: virtual machine name (must be unique azure name)")
	dbConfigPtr := flag.String("d", "None", "optional: local mongo db config to update local vm info")
	flag.Parse()

	vmName := *vmNamePtr
	dbConfig := *dbConfigPtr

	// load config, requires clientID, userAgent, SubscriptionID, GroupName
	config.Environment()

	// delete instances
	ctx := context.Background()
	resID := *vmutils.GetVM(ctx, vmutils.GetVMClient(), vmName).ID
	vmutils.DeleteInstance(ctx, config.GroupName(), vmName)

	if dbConfig != "None" {
		db := spdb.InitMongoDB(dbConfig)
		spdb.DeleteVMByID(db, spdb.VMCollection, resID)
	}
}
