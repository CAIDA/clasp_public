package vmutils

import (
	"context"
	"fmt"
	"log"
	"sort"
	"spservers/spdb"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
)

var wg sync.WaitGroup

type kv struct {
	Key   string
	Value int
}

// HandleGCPErr log the err and stop the program
func HandleGCPErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

/* Getter */

// GetInstance get instance info
func GetInstance(ctx context.Context, project, zone, instance string) *compute.Instance {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	resp, err := computeService.Instances.Get(project, zone, instance).Context(ctx).Do()
	HandleGCPErr(err)

	return resp
}

// ListInstances list all the vms under zone
func ListInstances(ctx context.Context, zone, project string) []*compute.Instance {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	res := []*compute.Instance{}
	req := computeService.Instances.List(project, zone)
	err = req.Pages(ctx, func(page *compute.InstanceList) error {
		for _, instance := range page.Items {
			res = append(res, instance)
		}
		return nil
	})
	HandleGCPErr(err)

	return res
}

// GetAvailableMachineType get available machine type under the zone
func GetAvailableMachineType(ctx context.Context, zone, project string) []*compute.MachineType {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	res := []*compute.MachineType{}
	// fetch machine types
	req := computeService.MachineTypes.List(project, zone)
	err = req.Pages(ctx, func(page *compute.MachineTypeList) error {
		for _, machineType := range page.Items {
			res = append(res, machineType)
		}
		return nil
	})
	HandleGCPErr(err)

	return res
}

// GetZones get available zones
func GetZones(ctx context.Context, project string) []*compute.Zone {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	res := []*compute.Zone{}
	req := computeService.Zones.List(project)
	err = req.Pages(ctx, func(page *compute.ZoneList) error {
		for _, zone := range page.Items {
			res = append(res, zone)
		}
		return nil
	})
	HandleGCPErr(err)

	return res
}

// GetZoneCount count zones of current region return (zone -> number of vms in this zone)
func GetZoneCount(ctx context.Context, zones []string, project string) map[string]int {
	// init zone count
	zoneMap := make(map[string]int)
	for _, val := range zones {
		zoneMap[val] = 0
	}

	// count zones
	for _, zone := range zones {
		vms := ListInstances(ctx, zone, project)
		zoneMap[zone] += len(vms)
	}

	return zoneMap
}

// GetNextAvailableZone get next available zone and assigned it to the vm
func GetNextAvailableZone(ctx context.Context, project, region string) string {
	allZones := GetZones(ctx, project)
	zoneNames := []string{}
	for _, ele := range allZones {
		zoneNames = append(zoneNames, ele.Name)
	}

	zoneNames = Filter(zoneNames, region, func(name, region string) bool {
		return strings.HasPrefix(name, region)
	})

	zoneCnt := GetZoneCount(ctx, zoneNames, project)

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

// InstanceToVMInfo convert instance type to vmInfo type
func InstanceToVMInfo(instances []*compute.Instance) []spdb.VMInfo {
	vms := []spdb.VMInfo{}
	for _, instance := range instances {
		vm := spdb.VMInfo{
			Type:   "gcp",
			Name:   instance.Name,
			ID:     strconv.Itoa(int(instance.Id)),
			Status: instance.Status,
			Zone:   instance.Zone,
			Ipv4:   instance.NetworkInterfaces[0].AccessConfigs[0].NatIP,
			DNS:    "N/A",
		}
		vms = append(vms, vm)
	}

	return vms
}

// GetInstanceInfo return instances information in spdb.VMInfo format
func GetInstanceInfo(ctx context.Context, zone, project string) []spdb.VMInfo {
	instances := ListInstances(ctx, zone, project)
	vmInfoArr := InstanceToVMInfo(instances)
	return vmInfoArr
}

/* Setter */

// StartInstance start instance with given name and give zone, return the instance id
func StartInstance(ctx context.Context, instance, zone, project string) string {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	// launch op
	resp, err := computeService.Instances.Start(project, zone, instance).Context(ctx).Do()
	HandleGCPErr(err)

	log.Println("Start Instance", resp.TargetId, "Status Code:", resp.ServerResponse.HTTPStatusCode,
		"Current Operation Status:", resp.Status)
	return strconv.Itoa(int(resp.TargetId))
}

// StopInstance start instance with given name and give zone, return the instance id
func StopInstance(ctx context.Context, instance, zone, project string) string {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	// launch op
	resp, err := computeService.Instances.Stop(project, zone, instance).Context(ctx).Do()
	HandleGCPErr(err)

	log.Println("Stop Instance", resp.TargetId, "Status Code:", resp.ServerResponse.HTTPStatusCode,
		"Current Operation Status:", resp.Status)
	return strconv.Itoa(int(resp.TargetId))
}

// CreateInstance create vm with specified parameters, by default, machine type will be n1-standard-2
// or n2-standard-2
func CreateInstance(ctx context.Context, project, zone, name string) {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	// find next machine
	machineTypes := GetAvailableMachineType(ctx, zone, project)
	targetType := "n1-standard-2"
	for _, ele := range machineTypes {
		if ele.Name == "n2-standard-2" {
			targetType = "n2-standard-2"
		}
	}

	// specify cpu
	var cpuType string
	if targetType == "n1-standard-2" {
		cpuType = "Intel Broadwell"
	} else {
		cpuType = "Intel Cascade Lake"
	}

	rb := &compute.Instance{
		Name:           name,
		MachineType:    "zones/" + zone + "/machineTypes/" + targetType,
		MinCpuPlatform: cpuType,
		Disks: []*compute.AttachedDisk{
			&compute.AttachedDisk{
				AutoDelete: true,
				Boot:       true,
				InitializeParams: &compute.AttachedDiskInitializeParams{
					DiskSizeGb:  30,
					SourceImage: "projects/gce-uefi-images/global/images/ubuntu-1804-bionic-v20200317",
				},
			},
		},
		NetworkInterfaces: []*compute.NetworkInterface{
			&compute.NetworkInterface{
				AccessConfigs: []*compute.AccessConfig{
					&compute.AccessConfig{
						NetworkTier: "PREMIUM",
					},
				},
			},
		},
	}

	resp, err := computeService.Instances.Insert(project, zone, rb).Context(ctx).Do()
	HandleGCPErr(err)

	log.Println("Created Instance", resp.TargetId, "Status Code:", resp.ServerResponse.HTTPStatusCode,
		"Current Operation Status:", resp.Status)
}

// DeleteInstance start instance with given name and give zone, return the instance id
// disk will be deleted since autodelete enabled
func DeleteInstance(ctx context.Context, instance, zone, project string) string {
	c, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	HandleGCPErr(err)

	computeService, err := compute.New(c)
	HandleGCPErr(err)

	// launch op
	resp, err := computeService.Instances.Delete(project, zone, instance).Context(ctx).Do()
	HandleGCPErr(err)

	log.Println("Delete Instance", resp.TargetId, "Status Code:", resp.ServerResponse.HTTPStatusCode,
		"Current Operation Status:", resp.Status)
	return strconv.Itoa(int(resp.TargetId))
}

/* DB helper functions */

// UpdateLocalVMInfo update local vm information
func UpdateLocalVMInfo(ctx context.Context, dbConfigPath, project string) {
	db := spdb.InitMongoDB(dbConfigPath)
	filter := bson.D{{"type", "gcp"}}
	spdb.ClearCollection(*db.Database.Collection(spdb.VMCollection), filter)
	for _, zone := range GetZones(ctx, project) {
		wg.Add(1)
		go func(zoneName string) {
			defer wg.Done()
			vmInfoArr := GetInstanceInfo(ctx, zoneName, project)
			spdb.InsertInstanceIDName(db, vmInfoArr, spdb.VMCollection)
		}(zone.Name)
	}
	wg.Wait()
}

// Filter filtering string matching the condition
func Filter(vs []string, b string, f func(a, b string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v, b) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}
