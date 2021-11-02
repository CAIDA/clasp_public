package ec2utils

import (
	"fmt"
	"log"
	"sort"
	"spservers/spdb"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"go.mongodb.org/mongo-driver/bson"
)

/* EC2 utils */

// CreateVMInput struct to specify create vm input
type CreateVMInput struct {
	ImageID       string
	Type          string
	Name          string
	KeyName       string
	SecurityGroup string
}

type kv struct {
	Key   string
	Value int
}

// HandleError print the error and stop the program
func HandleError(err error) {
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Fatal(aerr.Error())
			}
		} else {
			log.Fatal(err.Error())
		}
		return
	}
}

/* Getters */

// GetAvailableZones get zones matching specified parameters
func GetAvailableZones(svc *ec2.EC2, zoneNames []*string, displayAll bool, filters []*ec2.Filter) []string {
	var res *ec2.DescribeAvailabilityZonesOutput
	var err error
	if displayAll {
		res, err = svc.DescribeAvailabilityZones(&ec2.DescribeAvailabilityZonesInput{
			AllAvailabilityZones: &displayAll,
		})
	} else {
		res, err = svc.DescribeAvailabilityZones(&ec2.DescribeAvailabilityZonesInput{
			AllAvailabilityZones: &displayAll,
			ZoneNames:            zoneNames,
			Filters:              filters,
		})
	}
	HandleError(err)

	zones := []string{}
	for _, zone := range res.AvailabilityZones {
		zones = append(zones, *zone.ZoneName)
	}

	return zones
}

// GetAllInstancesInfo get all instance info
func GetAllInstancesInfo(svc *ec2.EC2) []spdb.VMInfo {
	continueToken := ""
	vmInfoArr := []spdb.VMInfo{}

	for {
		input := &ec2.DescribeInstancesInput{
			MaxResults: aws.Int64(1000),
		}

		if continueToken != "" {
			input.SetNextToken(continueToken)
		}

		res, err := svc.DescribeInstances(input)
		HandleError(err)

		// get all instance id
		reservations := res.Reservations
		for _, reservation := range reservations {
			for _, instance := range reservation.Instances {
				info := spdb.VMInfo{
					Type:   "aws",
					ID:     *instance.InstanceId,
					Status: *instance.State.Name,
					Zone:   *instance.Placement.AvailabilityZone,
					Name:   "N/A",
					Ipv4:   "N/A",
					DNS:    "N/A",
				}

				// fill in ipv4
				if instance.PublicIpAddress != nil {
					info.Ipv4 = *instance.PublicIpAddress
				}

				// fill in DNS
				if instance.PublicDnsName != nil {
					info.DNS = *instance.PublicDnsName
				}

				// find name tag
				tags := instance.Tags
				for _, v := range tags {
					if *v.Key == "Name" {
						info.Name = *v.Value
						break
					}
				}

				vmInfoArr = append(vmInfoArr, info)
			}
		}

		if res.NextToken == nil {
			break
		}
		continueToken = *res.NextToken
	}

	return vmInfoArr
}

// GetZoneCount Get count of zones in current user configered region
func GetZoneCount(svc *ec2.EC2) map[string]int {
	continueToken := ""
	zoneCnt := make(map[string]int)

	for {
		input := &ec2.DescribeInstancesInput{
			MaxResults: aws.Int64(1000),
		}

		if continueToken != "" {
			input.SetNextToken(continueToken)
		}

		res, err := svc.DescribeInstances(input)
		HandleError(err)

		// get all instance id
		reservations := res.Reservations
		for _, reservation := range reservations {
			for _, instance := range reservation.Instances {
				zone := *instance.Placement.AvailabilityZone
				if _, ok := zoneCnt[zone]; ok {
					zoneCnt[zone]++
				} else {
					zoneCnt[zone] = 1
				}
			}
		}

		if res.NextToken == nil {
			break
		}
		continueToken = *res.NextToken
	}

	return zoneCnt
}

// GetTagOfAllInstances get specified tag of all instances
func GetTagOfAllInstances(svc *ec2.EC2, tag string) []ec2.TagDescription {
	res := []ec2.TagDescription{}
	continueToken := ""
	for {
		input := &ec2.DescribeTagsInput{
			Filters: []*ec2.Filter{
				{
					Name: aws.String("key"),
					Values: []*string{
						aws.String(tag),
					},
				},
			},
			MaxResults: aws.Int64(1000),
		}

		if continueToken != "" {
			input.SetNextToken(continueToken)
		}

		result, err := svc.DescribeTags(input)
		HandleError(err)

		// append tags of instances to res array
		for _, tag := range result.Tags {
			res = append(res, *tag)
		}

		// if already last page, just break and return
		if result.NextToken == nil {
			break
		}
		continueToken = *result.NextToken
	}

	return res
}

// get next zone for newly created vm, make sure number of vms at each region is balanced
func getNextAvailableZone(svc *ec2.EC2) string {
	zoneCnt := GetZoneCount(svc)
	zones := GetAvailableZones(svc, []*string{}, true, []*ec2.Filter{})

	// add zones availble but not used to zone cnt
	for _, zone := range zones {
		if _, ok := zoneCnt[zone]; !ok {
			zoneCnt[zone] = 0
		}
	}

	// sort zones by cnt
	var sortedZones []kv
	for k, v := range zoneCnt {
		sortedZones = append(sortedZones, kv{k, v})
	}

	sort.Slice(sortedZones, func(i, j int) bool {
		return sortedZones[i].Value < sortedZones[j].Value
	})

	fmt.Println(zoneCnt)
	return sortedZones[0].Key
}

/* Setters */

// StopInstance stop instance with provided instanceID
func StopInstance(svc *ec2.EC2, instanceID string) {
	input := &ec2.StopInstancesInput{
		InstanceIds: []*string{
			aws.String(instanceID),
		},
		DryRun: aws.Bool(true),
	}
	result, err := svc.StopInstances(input)
	awsErr, ok := err.(awserr.Error)

	// if dry run succeed, then stop instances
	if ok && awsErr.Code() == "DryRunOperation" {
		input.DryRun = aws.Bool(false)
		result, err = svc.StopInstances(input)
		HandleError(err)
		log.Println("Success", result.StoppingInstances)
	} else {
		log.Fatal("Error", err)
	}
}

// StartInstance start instance with provided insrtance ID
func StartInstance(svc *ec2.EC2, instanceID string) {
	// We set DryRun to true to check permission/existence
	input := &ec2.StartInstancesInput{
		InstanceIds: []*string{
			aws.String(instanceID),
		},
		DryRun: aws.Bool(true),
	}
	result, err := svc.StartInstances(input)
	awsErr, ok := err.(awserr.Error)

	if ok && awsErr.Code() == "DryRunOperation" {
		input.DryRun = aws.Bool(false)
		result, err = svc.StartInstances(input)
		HandleError(err)
		log.Println("Success", result.StartingInstances)
	} else {
		log.Fatal("Error", err)
	}
}

// DeleteInstance Delete vm with specified id
func DeleteInstance(svc *ec2.EC2, id string) {
	res, err := svc.TerminateInstances(&ec2.TerminateInstancesInput{
		DryRun: aws.Bool(false),
		InstanceIds: []*string{
			aws.String(id),
		},
	})
	HandleError(err)

	log.Printf("Successfully toggle instance %s from %s to %s",
		*res.TerminatingInstances[0].InstanceId,
		res.TerminatingInstances[0].PreviousState,
		res.TerminatingInstances[0].CurrentState)
}

// CreateEC2Instance create an instance with provided config info
// and update local vm database information
func CreateEC2Instance(sess *session.Session, config CreateVMInput, pathToDBConfig string) {
	// Create EC2 service client
	svc := ec2.New(sess)
	zone := getNextAvailableZone(svc)
	createRes := createInstance(svc, config, zone)
	addTagToInstance(svc, createRes, config)

	if pathToDBConfig != "None" {
		instance := *createRes.Instances[0]

		// block till received a valid network interface
		nicInfoChan := make(chan *ec2.InstanceNetworkInterfaceAssociation)

		// get ip/dns
		go func() {
			for {
				time.Sleep(1 * time.Second)
				descRes, err := svc.DescribeInstances(&ec2.DescribeInstancesInput{
					InstanceIds: []*string{
						instance.InstanceId,
					},
				})
				HandleError(err)

				nicAssociation := descRes.Reservations[0].
					Instances[0].
					NetworkInterfaces[0].
					Association

				if nicAssociation != nil {
					nicInfoChan <- nicAssociation
					break
				}
			}
		}()

		nicAssociation := <-nicInfoChan
		vms := []spdb.VMInfo{spdb.VMInfo{
			Type:   "aws",
			Name:   config.Name,
			ID:     *instance.InstanceId,
			Ipv4:   *nicAssociation.PublicIp,
			DNS:    *nicAssociation.PublicDnsName,
			Zone:   zone,
			Status: *instance.State.Name}}

		db := spdb.InitMongoDB(pathToDBConfig)
		spdb.InsertInstanceIDName(db, vms, spdb.VMCollection)
	}
}

/* Helper Functions & DB functions */
func createInstance(svc *ec2.EC2, config CreateVMInput, zone string) *ec2.Reservation {
	// Specify the details of the instance that you want to create.
	runResult, err := svc.RunInstances(&ec2.RunInstancesInput{
		// An Amazon Linux AMI ID for t2.micro instances in the us-west-2 region
		ImageId:      aws.String(config.ImageID),
		InstanceType: aws.String(config.Type),
		MinCount:     aws.Int64(1),
		MaxCount:     aws.Int64(1),
		Placement: &ec2.Placement{
			AvailabilityZone: aws.String(zone),
		},
		KeyName: aws.String(config.KeyName),
		SecurityGroups: []*string{
			aws.String(config.SecurityGroup),
		},
	})

	if err != nil {
		log.Fatal("Could not create instance", err)
	}

	log.Println("Created instance", *runResult.Instances[0].InstanceId)
	return runResult
}

func addTagToInstance(svc *ec2.EC2, runResult *ec2.Reservation, config CreateVMInput) {
	// Add tags to the created instance
	_, errtag := svc.CreateTags(&ec2.CreateTagsInput{
		Resources: []*string{runResult.Instances[0].InstanceId},
		Tags: []*ec2.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(config.Name),
			},
		},
	})
	if errtag != nil {
		log.Println("Could not create tags for instance", runResult.Instances[0].InstanceId, errtag)
		return
	}

	log.Println("Successfully tagged instance")
}

// CreateMapWithTagDesc create map from name -> instance id
func CreateMapWithTagDesc(tags []ec2.TagDescription) map[string]string {
	m := make(map[string]string)

	for _, tag := range tags {
		m[*tag.Value] = *tag.ResourceId
	}

	return m
}

// UpdateLocalVMInfo update local vm database with the most
// recent instance information on EC2 console and ensure provided index
func UpdateLocalVMInfo(svc *ec2.EC2, dbConfigPath string) {
	db := spdb.InitMongoDB(dbConfigPath)
	filter := bson.D{{"type", "aws"}}
	spdb.ClearCollection(*db.Database.Collection(spdb.VMCollection), filter)
	vms := GetAllInstancesInfo(svc)
	spdb.CreateIndex(*db.Database.Collection(spdb.VMCollection), "id")
	spdb.InsertInstanceIDName(db, vms, spdb.VMCollection)
}
