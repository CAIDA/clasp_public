package main

import (
	"cloudutils/aws/ec2/ec2utils"
	"flag"
	"spservers/spdb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func main() {
	// parse arguments
	vmNamePtr := flag.String("v", "None", "required: virtual machine name (must be unique ec2 name tag)")
	dbConfigPtr := flag.String("d", "None", "optional: local mongo db config to update local vm info")
	flag.Parse()

	vmName := *vmNamePtr
	dbConfig := *dbConfigPtr

	// Create EC2 service client
	sess, err := session.NewSession()
	ec2utils.HandleError(err)
	svc := ec2.New(sess)

	// Get id of name
	res, err := svc.DescribeInstances(&ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("tag:Name"),
				Values: []*string{
					aws.String(vmName),
				},
			},
		},
		MaxResults: aws.Int64(1000),
	})
	ec2utils.HandleError(err)
	instance := res.Reservations[0].Instances[0]
	ec2utils.DeleteInstance(svc, *instance.InstanceId)

	if dbConfig != "None" {
		db := spdb.InitMongoDB(dbConfig)
		spdb.DeleteVMByID(db, spdb.VMCollection, *instance.InstanceId)
	}
}
