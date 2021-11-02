package main

import (
	"cloudutils/aws/ec2/ec2utils"
	"flag"
	"log"
	"spservers/spdb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func main() {
	// parse arguments
	opPtr := flag.String("o", "None", "required: start/stop")
	vmNamePtr := flag.String("v", "None", "required: virtual machine name (must be unique ec2 name tag)")
	dbConfigPtr := flag.String("d", "None", "optional: local mongo db config to update local vm info")
	flag.Parse()

	op := *opPtr
	vmName := *vmNamePtr
	dbConfig := *dbConfigPtr

	// do operation
	if op != "start" && op != "stop" {
		log.Fatal("Please provide start/stop as the operation command")
	}

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

	var pStatus string
	switch op {
	case "start":
		ec2utils.StartInstance(svc, *instance.InstanceId)
		pStatus = "running"
	case "stop":
		ec2utils.StopInstance(svc, *instance.InstanceId)
		pStatus = "stopped"
	}

	if dbConfig != "None" {
		db := spdb.InitMongoDB(dbConfig)
		spdb.UpdateState(db, "vmInfo", pStatus, *instance.InstanceId)
	}
}
