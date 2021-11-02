package main

import (
	ec2utils "cloudutils/aws/ec2/ec2utils"
	"cloudutils/azure/vm/config"
	azureutils "cloudutils/azure/vm/vmutils"
	gcputils "cloudutils/gcp/vm/vmutils"
	"context"
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func main() {
	log.Println("Updating gcp/azure/ec2 vm informations")
	config.Environment()
	sess, err := session.NewSession()
	ec2utils.HandleError(err)
	svc := ec2.New(sess)
	azureutils.UpdateLocalVMInfo("./dbconfig.json", "id")
	ec2utils.UpdateLocalVMInfo(svc, "./dbconfig.json")
	gcputils.UpdateLocalVMInfo(context.Background(), "./dbconfig.json", "webspeedtest-caida")
}
