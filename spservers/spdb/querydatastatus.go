package spdb

import (
	"context"
	"errors"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (cm *SpeedtestMongo) QueryDataStatus(mon string) (*VMDataStatus, error) {
	if cm.Database != nil {
		cdata := cm.Database.Collection(Coldatastatus)
		filter := bson.D{{"mon", mon}}
		var status VMDataStatus
		var err error
		res := cdata.FindOne(context.TODO(), filter)
		if res.Err() == mongo.ErrNoDocuments {
			return &VMDataStatus{}, nil
		} else {
			err = res.Decode(&status)
		}
		return &status, err
	} else {
		return nil, errors.New("Database is nil")
	}
}

func (cm *SpeedtestMongo) UpdateDataStatus(vmstatus *VMDataStatus) {
	cdata := cm.Database.Collection(Coldatastatus)
	opt := options.FindOneAndReplace().SetUpsert(true)
	filter := bson.D{{"mon", vmstatus.Mon}}
	res := cdata.FindOneAndReplace(context.TODO(), filter, vmstatus, opt)
	if res.Err() != nil {
		if res.Err() != mongo.ErrNoDocuments {
			//	log.Println("Server not found", ser.Type, ser.Id)
			log.Fatal(res.Err())
		}
	}

}
