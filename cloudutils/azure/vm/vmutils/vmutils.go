package vmutils

import (
	"cloudutils/azure/vm/config"
	"cloudutils/azure/vm/iam"
	"cloudutils/azure/vm/network"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"spservers/spdb"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2020-06-01/compute"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/to"
	"go.mongodb.org/mongo-driver/bson"
)

type kv struct {
	Key   string
	Value int
}

var wg1 sync.WaitGroup

// AzureHandleErr stop the program and print the error if the error is not nil
func AzureHandleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

/* Getter */

// GetVMClient get client for VM management
func GetVMClient() compute.VirtualMachinesClient {
	vmClient := compute.NewVirtualMachinesClient(config.SubscriptionID())
	auth := iam.GetAuthFromEnv()
	vmClient.Authorizer = auth
	vmClient.AddToUserAgent(config.UserAgent())
	return vmClient
}

//GetDiskClient return a DiskClient
func GetDiskClient() compute.DisksClient {
	diskClient := compute.NewDisksClient(config.SubscriptionID())
	auth := iam.GetAuthFromEnv()
	diskClient.Authorizer = auth
	diskClient.AddToUserAgent(config.UserAgent())
	return diskClient
}

// GetVM gets the specified VM info
func GetVM(ctx context.Context, vmClient compute.VirtualMachinesClient, vmName string) compute.VirtualMachine {
	res, err := vmClient.Get(ctx, config.GroupName(), vmName, compute.InstanceView)
	AzureHandleErr(err)
	return res
}

// ListVM gets all the vm information for a specified resource group
func ListVM(ctx context.Context, vmClient compute.VirtualMachinesClient, resourceGroupName string) []compute.VirtualMachine {
	res, err := vmClient.List(ctx, resourceGroupName)
	AzureHandleErr(err)
	vmList := []compute.VirtualMachine{}

	for res.NotDone() {
		vmList = append(vmList, res.Values()...)
		res.NextWithContext(ctx)
	}

	return vmList
}

// GetAvailableZones get available zones under user's default config region
func GetAvailableZones(subscriptionID, locationFilter, machineType string) []string {
	client := compute.NewResourceSkusClient(subscriptionID)
	auth := iam.GetAuthFromEnv()
	client.Authorizer = auth
	res, err := client.List(context.Background(), locationFilter)
	AzureHandleErr(err)
	var zones []string = []string{}

	for _, ele := range res.Values() {
		if *ele.Name == machineType {
			zones = *((*ele.LocationInfo)[0].Zones)
		}
	}

	for {
		if res.NotDone() {
			err := res.Next()
			AzureHandleErr(err)

			for _, ele := range res.Values() {
				if *ele.Name == machineType {
					zones = *((*ele.LocationInfo)[0].Zones)
				}
			}

			continue
		}
		break
	}

	return zones
}

// GetZoneCount get (loaction->vm count) mapping of current resource group
func GetZoneCount(resourceGroupName, location string, zones []string) map[string]int {
	vms := ListVM(context.Background(), GetVMClient(), resourceGroupName)
	zoneMap := make(map[string]int)

	// init zone count
	for _, val := range zones {
		zoneMap[val] = 0
	}

	for _, vm := range vms {
		if *vm.Location == location {
			if vm.Zones != nil {
				zoneMap[(*vm.Zones)[0]]++
			}
		}
	}

	return zoneMap
}

// GetIPDNS Get all ip configuration of a vm
func GetIPDNS(ctx context.Context, vm compute.VirtualMachine) (string, string) {
	// assign ip and dns
	nicRef := *vm.VirtualMachineProperties.NetworkProfile.NetworkInterfaces
	IPAddr := "N/A"
	DNS := "N/A"

	// extract the ip addr/DNS, assume that each vm uses one ip configuration
	if len(nicRef) > 0 {
		temp := strings.Split(*nicRef[0].ID, "/")
		nicName := temp[len(temp)-1]
		nicIf, err := network.GetNic(context.Background(), nicName)
		AzureHandleErr(err)

		IPConfig := (*(*nicIf.InterfacePropertiesFormat).IPConfigurations)
		if len(IPConfig) > 0 {
			if IPConfig[0].InterfaceIPConfigurationPropertiesFormat.PublicIPAddress.ID != nil {
				publicIPID := *IPConfig[0].
					InterfaceIPConfigurationPropertiesFormat.PublicIPAddress.ID
				temp := strings.Split(publicIPID, "/")
				ipName := temp[len(temp)-1]
				res, err := network.GetPublicIP(ctx, ipName)
				AzureHandleErr(err)

				if res.PublicIPAddressPropertiesFormat != nil &&
					res.PublicIPAddressPropertiesFormat.IPAddress != nil {
					IPAddr = *res.PublicIPAddressPropertiesFormat.IPAddress
				}

				if res.PublicIPAddressPropertiesFormat.DNSSettings != nil &&
					res.PublicIPAddressPropertiesFormat.DNSSettings.Fqdn != nil {
					DNS = *res.PublicIPAddressPropertiesFormat.DNSSettings.Fqdn
				}
			}
		}
	}
	return IPAddr, DNS
}

// GetPowerStatus get power status of vm
func GetPowerStatus(ctx context.Context, vm compute.VirtualMachine) string {
	// assign status
	statuses := GetVM(context.Background(), GetVMClient(), *vm.Name).
		VirtualMachineProperties.InstanceView.Statuses

	var res string = "N/A"
	for _, status := range *statuses {
		if strings.HasPrefix(*status.Code, "PowerState") {
			res = strings.Split(*status.Code, "/")[1]
		}
	}

	return res
}

// GetNextZone get next location for newly created vm
func GetNextZone(resourceGroup, location, subscriptionID string) string {
	allZones := GetAvailableZones(subscriptionID,
		"location eq "+"'"+location+"'", "Standard_D2s_v3")

	zoneCnt := GetZoneCount(resourceGroup, location, allZones)

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

/* Setter */

// StartInstance starts the selected VM
func StartInstance(ctx context.Context, vmName string) (osr string) {
	vmClient := GetVMClient()
	future, err := vmClient.Start(ctx, config.GroupName(), vmName)
	AzureHandleErr(err)

	err = future.WaitForCompletionRef(ctx, vmClient.Client)
	AzureHandleErr(err)

	res, err := future.Result(vmClient)
	AzureHandleErr(err)
	return res.Status
}

// StopInstance stops the selected VM
func StopInstance(ctx context.Context, vmName string) (osr string) {
	vmClient := GetVMClient()
	future, err := vmClient.PowerOff(ctx, config.GroupName(), vmName, nil)
	AzureHandleErr(err)

	err = future.WaitForCompletionRef(ctx, vmClient.Client)
	AzureHandleErr(err)

	res, err := future.Result(vmClient)
	AzureHandleErr(err)
	return res.Status
}

// CreateInstance creates a new virtual machine with the specified name using the specified NIC.
// Username, password, and sshPublicKeyPath determine logon credentials.
func CreateInstance(ctx context.Context, vmName, nicName, sshPublicKeyPath, zone string) (vm compute.VirtualMachine, err error) {
	// see the network samples for how to create and get a NIC resource
	nic, err := network.GetNic(ctx, nicName)
	AzureHandleErr(err)

	// read ssh data
	var sshKeyData string
	if _, err = os.Stat(sshPublicKeyPath); err == nil {
		sshBytes, err := ioutil.ReadFile(sshPublicKeyPath)
		if err != nil {
			log.Fatalf("failed to read SSH key data: %v", err)
		}
		sshKeyData = string(sshBytes)
	} else {
		sshKeyData = ""
	}

	// configure vm information
	vmClient := GetVMClient()
	vmInfo := compute.VirtualMachine{
		Location: to.StringPtr(config.Location()),
		Zones:    &[]string{zone},
		VirtualMachineProperties: &compute.VirtualMachineProperties{
			HardwareProfile: &compute.HardwareProfile{
				VMSize: compute.VirtualMachineSizeTypesStandardD2sV3,
			},
			StorageProfile: &compute.StorageProfile{
				ImageReference: &compute.ImageReference{
					Publisher: to.StringPtr("Canonical"),
					Offer:     to.StringPtr("UbuntuServer"),
					Sku:       to.StringPtr("18.04-LTS"),
					Version:   to.StringPtr("latest"),
				},
			},
			OsProfile: &compute.OSProfile{
				ComputerName:  to.StringPtr(vmName),
				AdminUsername: to.StringPtr("ubuntu"),
				LinuxConfiguration: &compute.LinuxConfiguration{
					SSH: &compute.SSHConfiguration{
						PublicKeys: &[]compute.SSHPublicKey{
							{
								Path: to.StringPtr(
									fmt.Sprintf("/home/ubuntu/.ssh/authorized_keys")),
								KeyData: to.StringPtr(sshKeyData),
							},
						},
					},
				},
			},
			NetworkProfile: &compute.NetworkProfile{
				NetworkInterfaces: &[]compute.NetworkInterfaceReference{
					{
						ID: nic.ID,
						NetworkInterfaceReferenceProperties: &compute.NetworkInterfaceReferenceProperties{
							Primary: to.BoolPtr(true),
						},
					},
				},
			},
		},
	}

	future, err := vmClient.CreateOrUpdate(
		ctx,
		config.GroupName(),
		vmName,
		vmInfo,
	)

	if err != nil {
		return vm, fmt.Errorf("cannot create vm: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, vmClient.Client)
	if err != nil {
		return vm, fmt.Errorf("cannot get the vm create or update future response: %v", err)
	}

	return future.Result(vmClient)
}

// DeallocateInstance deallocates the selected VM
func DeallocateInstance(ctx context.Context, vmName string) (osr autorest.Response, err error) {
	vmClient := GetVMClient()
	future, err := vmClient.Deallocate(ctx, config.GroupName(), vmName)
	if err != nil {
		return osr, fmt.Errorf("cannot deallocate vm: %v", err)
	}

	err = future.WaitForCompletionRef(ctx, vmClient.Client)
	if err != nil {
		return osr, fmt.Errorf("cannot get the vm deallocate future response: %v", err)
	}

	return future.Result(vmClient)
}

// DeleteDisk Delete disk with specified name
func DeleteDisk(ctx context.Context, diskName string) {
	client := GetDiskClient()
	fmt.Println(client)
	future, err := client.Delete(ctx, config.GroupName(), diskName)
	AzureHandleErr(err)
	err = future.WaitForCompletionRef(ctx, client.Client)
	AzureHandleErr(err)
	log.Println("Successfully deleted disk", diskName)
}

// DeleteInstance delete vm and its network interface, ip and disk
func DeleteInstance(ctx context.Context, resourceGroupName, vmName string) {
	client := GetVMClient()
	vm := GetVM(ctx, client, vmName)
	DeallocateInstance(ctx, vmName)

	// delete vm
	vmDeletefuture, err := client.Delete(ctx, resourceGroupName, vmName)
	AzureHandleErr(err)
	err = vmDeletefuture.WaitForCompletionRef(ctx, client.Client)
	AzureHandleErr(err)

	// delete os disk
	osDisk := *vm.VirtualMachineProperties.StorageProfile.OsDisk.Name
	DeleteDisk(ctx, osDisk)

	// delete nic
	nics := vm.VirtualMachineProperties.NetworkProfile.NetworkInterfaces
	for _, nicRef := range *nics {
		temp := strings.Split(*nicRef.ID, "/")
		nicName := temp[len(temp)-1]
		nic, err := network.GetNic(ctx, nicName)
		AzureHandleErr(err)
		nicDeleteFuture, err := network.DeleteNic(ctx, nicName)
		AzureHandleErr(err)
		err = nicDeleteFuture.WaitForCompletionRef(ctx, client.Client)
		AzureHandleErr(err)

		// delete virtual network
		subnet := (*(nic.InterfacePropertiesFormat.
			IPConfigurations))[0].
			InterfaceIPConfigurationPropertiesFormat.
			Subnet

		// find virtual network name
		temp = strings.Split(*subnet.ID, "/")
		var virtualNetwork string
		var idx int
		for i, ele := range temp {
			if ele == "virtualNetworks" {
				idx = i
			}
		}
		virtualNetwork = temp[idx+1]
		vnetDeleteFuture, err := network.DeleteVirtualNetwork(ctx, virtualNetwork)
		AzureHandleErr(err)
		err = vnetDeleteFuture.WaitForCompletionRef(ctx, client.Client)
		AzureHandleErr(err)

		// delete ip addr
		publicIPID := (*(nic.InterfacePropertiesFormat.
			IPConfigurations))[0].
			InterfaceIPConfigurationPropertiesFormat.
			PublicIPAddress.ID

		if publicIPID != nil {
			temp = strings.Split(*publicIPID, "/")
			ip := temp[len(temp)-1]
			ipDeleteFuture, err := network.DeletePublicIP(ctx, ip)
			AzureHandleErr(err)
			err = ipDeleteFuture.WaitForCompletionRef(ctx, client.Client)
			AzureHandleErr(err)
		}
	}

	log.Println("Successfully deleted", vmName, "and all its associated resources")
}

// UpdateLocalVMInfo store/udpate azure vm information locally
func UpdateLocalVMInfo(dbConfigPath string, index string) {
	db := spdb.InitMongoDB(dbConfigPath)

	filter := bson.D{{"type", "azure"}}
	spdb.ClearCollection(*db.Database.Collection(spdb.VMCollection), filter)

	res := ListVM(context.Background(), GetVMClient(), "ricky_speedtest")
	vms := []spdb.VMInfo{}
	for _, vm := range res {
		wg1.Add(1)
		go func(vm compute.VirtualMachine) {
			defer wg1.Done()
			vmInfo := spdb.VMInfo{
				Type: "azure",
				Name: *vm.Name,
				ID:   *vm.ID,
				Zone: *(vm.Location),
				Ipv4: "N/A",
				DNS:  "N/A",
			}
			// get zones
			if vm.Zones != nil {
				vmInfo.Zone += ("-" + (*vm.Zones)[0])
			}

			// assign status
			statuses := GetVM(context.Background(), GetVMClient(), *vm.Name).
				VirtualMachineProperties.InstanceView.Statuses

			for _, status := range *statuses {
				if strings.HasPrefix(*status.Code, "PowerState") {
					vmInfo.Status = strings.Split(*status.Code, "/")[1]
				}
			}

			// get IP/DNS
			ipAddr, DNS := GetIPDNS(context.Background(), GetVM(context.Background(), GetVMClient(), *vm.Name))
			vmInfo.Ipv4, vmInfo.DNS = ipAddr, DNS
			fmt.Println(vmInfo)
			vms = append(vms, vmInfo)
		}(vm)
	}
	wg1.Wait()
	spdb.CreateIndex(*db.Database.Collection(spdb.VMCollection), index)
	spdb.InsertInstanceIDName(db, vms, spdb.VMCollection)
}

// GenerateNicForVMWithNewIP generate a nic based on vmName, with a new ip adddress
func GenerateNicForVMWithNewIP(vmName, region, zone string) string {
	// generae ip address
	ipRes, err := network.CreatePublicIP(context.Background(), vmName+"ip", zone)
	AzureHandleErr(err)
	ipName := *ipRes.Name

	// generate vnet
	vnetRes, err := network.CreateVirtualNetwork(context.Background(), vmName+"vent")
	AzureHandleErr(err)
	vnetName := *vnetRes.Name

	// generate subnet for vnet
	subnet, err := network.CreateVirtualNetworkSubnet(context.Background(), vnetName, "default")
	AzureHandleErr(err)
	subnetName := *subnet.Name

	// generate network interface
	nic, err := network.CreateNIC(context.Background(), vmName, vnetName, subnetName,
		"speedtestnsg-"+region, ipName, vmName+"nic")
	AzureHandleErr(err)

	return *nic.Name
}
