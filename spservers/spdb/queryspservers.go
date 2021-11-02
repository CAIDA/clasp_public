package spdb

import (
	"context"
	"errors"
	"log"
	"net"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (cm *SpeedtestMongo) ResetEnable(servertype string) {
	if cm.Database != nil {
		cspeed := cm.Database.Collection(Colserver)
		filter := bson.D{{"type", servertype}}
		update := bson.D{{"$set", bson.D{{"enabled", false}}}}
		res, err := cspeed.UpdateMany(context.TODO(), filter, update)
		if err != nil {
			log.Fatal(err)
		}
		if res.MatchedCount > 0 {
			log.Println("Distabled", res.MatchedCount, servertype, "servers")
		}
	}
}

func (cm *SpeedtestMongo) InsertServers(servers []SpeedServer) (int, error) {
	if cm.Database != nil && len(servers) > 0 {
		nodoc := 0
		cspeed := cm.Database.Collection(Colserver)
		opt := options.FindOneAndReplace().SetUpsert(true)
		for _, ser := range servers {
			filter := bson.D{{"type", ser.Type}, {"id", ser.Id}}
			res := cspeed.FindOneAndReplace(context.TODO(), filter, ser, opt)
			if res.Err() != nil {
				if res.Err() == mongo.ErrNoDocuments {
					nodoc++
					//	log.Println("Server not found", ser.Type, ser.Id)
				} else {
					log.Fatal(res.Err())
				}
			}
		}

		return nodoc, nil
	}
	if cm.Database == nil {
		return 0, errors.New("Database is nil")
	}
	return 0, nil
}
func (cm *SpeedtestMongo) QueryServersRaw(filters interface{}) (*mongo.Cursor, error) {
	if cm.Database != nil {
		cspeed := cm.Database.Collection(Colserver)
		return cspeed.Find(context.TODO(), filters)
	} else {
		return nil, errors.New("Database is nil")
	}
}

func (cm *SpeedtestMongo) QueryServerbyId(sid primitive.ObjectID) (*SpeedServer, error) {
	if cm.Database != nil {
		filter := bson.D{{"_id", sid}}
		cspeed := cm.Database.Collection(Colserver)
		var server SpeedServer
		err := cspeed.FindOne(context.TODO(), filter).Decode(&server)
		if err != nil {
			return nil, err
		}
		return &server, nil
	}
	return nil, errors.New("Database is nil")
}

func (cm *SpeedtestMongo) QueryEnabledServersbyType(stype string) ([]SpeedServer, error) {
	var allservers []SpeedServer
	filter := bson.D{{"type", stype}, {"enabled", true}}
	cursor, err := cm.QueryServersRaw(filter)
	err = cursor.All(context.TODO(), &allservers)
	return allservers, err
}

func (cm *SpeedtestMongo) QueryEnabledServers() ([]SpeedServer, error) {
	var allservers []SpeedServer
	filter := bson.D{{"enabled", true}}
	cursor, err := cm.QueryServersRaw(filter)
	err = cursor.All(context.TODO(), &allservers)
	return allservers, err
}

func (cm *SpeedtestMongo) QueryServersbyIPv4(serverip net.IP) ([]SpeedServer, error) {
	var allservers []SpeedServer
	filter := bson.D{{"ipv4", serverip.String()}}
	cursor, err := cm.QueryServersRaw(filter)
	err = cursor.All(context.TODO(), &allservers)
	return allservers, err
}

func (cm *SpeedtestMongo) QueryServerbyIdentifier(stype, iden string) (SpeedServer, error) {
	spserver := SpeedServer{}
	if cm.Database != nil {
		cspeed := cm.Database.Collection(Colserver)
		filter := bson.D{{"type", stype}, {"identifier", iden}}
		err := cspeed.FindOne(context.TODO(), filter).Decode(&spserver)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return spserver, err
			}
		}
		return spserver, nil
	}
	return spserver, errors.New("Database is nil")
}
