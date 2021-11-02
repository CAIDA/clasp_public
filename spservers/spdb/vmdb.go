package spdb

import (
	"context"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

var wg1 sync.WaitGroup

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

// VMCollection name of the vm collection
var VMCollection string = "vmInfo"

// AzureVMCollection name of local collection storing azure vminfo
var AzureVMCollection string = "azureVmInfo"

// VMInfo record containing information of VM
type VMInfo struct {
	// VM type: azure/aws/gcp
	Type string
	// Name name of vm
	Name string
	// Id instance id
	ID string
	// Status status of the vm
	Status string
	// Ipv4 ip addr
	Ipv4 string
	// DNS
	DNS string
	// Zone availability zone
	Zone string
}

/* Mongo utils */

// InitMongoDB initialize a mongodb client
func InitMongoDB(mongoConfigFile string) *SpeedtestMongo {
	db := NewMongoDB(mongoConfigFile, "speedtest")
	if db.Client == nil {
		log.Fatal("Connect to mongodb err")
	}
	return db
}

// CreateIndex ensure index for collection
func CreateIndex(collection mongo.Collection, field string) {
	_, err := collection.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys:    bsonx.Doc{{field, bsonx.Int32(1)}},
			Options: options.Index().SetUnique(true),
		},
	)
	handleError(err)
}

// GetCollection get collection by name, if not exists, create one
func GetCollection(cm *SpeedtestMongo, collection string) *mongo.Collection {
	return cm.Database.Collection(collection)
}

// InsertInstanceIDName insert id and name of vm as a record to db (for aws ec2 instances)
func InsertInstanceIDName(cm *SpeedtestMongo, VMs []VMInfo, collection string) {
	if cm.Database != nil && len(VMs) > 0 {
		collection := GetCollection(cm, collection)

		// insert all VM in {name, instanceID} format
		interfaceSlice := make([]interface{}, len(VMs))
		for i, d := range VMs {
			interfaceSlice[i] = d
		}

		opt := options.Update().SetUpsert(true)
		for _, ele := range VMs {
			wg1.Add(1)
			go func(ele VMInfo) {
				defer wg1.Done()
				update := bson.D{{"$set", bson.D{
					{"type", ele.Type},
					{"id", ele.ID},
					{"name", ele.Name},
					{"ipv4", ele.Ipv4},
					{"dns", ele.DNS},
					{"zone", ele.Zone},
					{"status", ele.Status},
				}}}
				filter := bson.D{{"id", ele.ID}}
				res, err := collection.UpdateOne(context.Background(), filter, update, opt)
				handleError(err)
				log.Println("inserted ids:", res.UpsertedID)
				log.Println("Updated count:", res.ModifiedCount)
			}(ele)
		}
		wg1.Wait()
	}
}

// UpdateState update state of vm
func UpdateState(cm *SpeedtestMongo, collection string, state string, instanceID string) {
	if cm.Database != nil {
		collection := GetCollection(cm, collection)

		// insert all VM in {name, instanceID} format
		opt := options.Update()
		update := bson.D{{"$set", bson.D{
			{"status", state},
		}}}
		filter := bson.D{{"id", instanceID}}
		res, err := collection.UpdateOne(context.Background(), filter, update, opt)
		handleError(err)
		log.Println("update count:", res.ModifiedCount)
	}
}

// QueryVMIDByName query VM by its name, returning its id (for vm instances)
func QueryVMIDByName(cm *SpeedtestMongo, name string) string {
	// create a value into which the result can be decoded
	var id string

	collection := cm.Database.Collection(VMCollection)
	filter := bson.D{{"name", name}}
	err := collection.FindOne(context.TODO(), filter).Decode(&id)
	handleError(err)

	log.Printf("Found a single document: %+v\n", id)
	return id
}

// DeleteVMByID delete an vm by id
func DeleteVMByID(cm *SpeedtestMongo, collectionName, id string) string {
	collection := cm.Database.Collection(collectionName)
	filter := bson.D{{"id", id}}
	res, err := collection.DeleteOne(context.TODO(), filter)
	handleError(err)
	log.Println("Successfully deleted", res.DeletedCount, "documents")
	return id
}

// ClearCollection delete things inside a collection matching filter
func ClearCollection(collection mongo.Collection, filter bson.D) {
	//filter := bson.D{{}}
	res, err := collection.DeleteMany(context.Background(), filter)
	handleError(err)
	log.Println("Delete Result: ", res.DeletedCount)
}
