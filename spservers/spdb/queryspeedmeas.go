package spdb

import (
	"context"
	"errors"
	"log"
	"regexp"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (cm *SpeedtestMongo) InsertManySpeedMeas(spmes []*SpeedMeas) (int, error) {
	if cm.Database != nil {
		if len(spmes) > 0 {
			csm := cm.Database.Collection(Colspeedmeas)
			opts := options.InsertMany().SetOrdered(false)
			islice := make([]interface{}, len(spmes))
			for smidx, _ := range spmes {
				islice[smidx] = spmes[smidx]
			}
			res, err := csm.InsertMany(context.TODO(), islice, opts)
			if err != nil {
				log.Println(err)
				return 0, err
			}
			return len(res.InsertedIDs), nil
		}
	}
	return -1, errors.New("Database is nil")
}

func (cm *SpeedtestMongo) QuerySpeedserverExist(region string, spid primitive.ObjectID) (int, error) {
	if cm.Database != nil {
		csm := cm.Database.Collection(Colspeedmeas)
		monreg := regexp.MustCompile(`(\w+-\w+-)\w+`)
		regionarr := monreg.FindStringSubmatch(region)
		if len(regionarr) > 1 {
			var spmeas SpeedMeas
			filter := bson.D{{"mon", bson.D{{"$regex", regionarr[1] + "*"}}}, {"speedserver", spid}}
			err := csm.FindOne(context.TODO(), filter).Decode(&spmeas)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					return -1, nil
				} else {
					return -99, err
				}
			} else {
				if spmeas.Enabled {
					return 1, nil
				}
				return 0, nil
			}
		} else {
			return 0, errors.New("Monitor name patten does not match")
		}

	}
	return -99, errors.New("Database is nil")
}

type SpeedMeasAgg struct {
	Mon          string        `bson:"mon"`
	SpserverInfo []SpeedServer `bson:"spserver"`
	LinkInfo     []Link        `bson:"link"`
}

func (cm *SpeedtestMongo) QueryAllEnabledSpeedMeas() []*SpeedMeasAgg {
	if cm.Database != nil {
		csm := cm.Database.Collection(Colspeedmeas)
		pipeline := bson.A{
			bson.D{{"$match", bson.D{{"enabled", true}}}},
			bson.D{{"$lookup", bson.D{{"from", "speedserver"}, {"localField", "speedserver"}, {"foreignField", "_id"}, {"as", "spserver"}}}},
			bson.D{{"$lookup", bson.D{{"from", "links"}, {"localField", "link"}, {"foreignField", "_id"}, {"as", "link"}}}},
		}
		var allsmeas []*SpeedMeasAgg
		if cur, err := csm.Aggregate(context.TODO(), pipeline); err == nil {
			errc := cur.All(context.TODO(), &allsmeas)
			if errc == nil {
				return allsmeas
			}
		}
	}
	return nil

}

func (cm *SpeedtestMongo) QueryMapSpeedMeas() map[string][]*SpeedMeas {
	if cm.Database != nil {
		csm := cm.Database.Collection(Colspeedmeas)
		filter := bson.D{{}}
		var allsmeas []*SpeedMeas
		monreg := regexp.MustCompile(`(\w+-\w+)-\w+`)
		if cur, err := csm.Find(context.TODO(), filter); err == nil {
			errc := cur.All(context.TODO(), &allsmeas)
			if errc == nil {
				rmap := make(map[string][]*SpeedMeas)
				for smeasidx, smeas := range allsmeas {
					monarr := monreg.FindStringSubmatch(smeas.Mon)
					key := monarr[1] + ":" + smeas.SpeedServer.Hex()
					if _, rexist := rmap[key]; !rexist {
						rmap[key] = []*SpeedMeas{allsmeas[smeasidx]}
					} else {
						rmap[key] = append(rmap[key], allsmeas[smeasidx])
					}
				}
				return rmap
			}
		}
	}
	return nil
}

func (cm *SpeedtestMongo) UpdateSpeedserver(spmeas *SpeedMeas) error {
	if cm.Database != nil && spmeas != nil {
		csm := cm.Database.Collection(Colspeedmeas)
		opts := options.FindOneAndReplace().SetUpsert(true)
		filter := bson.D{{"mon", spmeas.Mon}, {"speedserver", spmeas.SpeedServer}, {"link", spmeas.Link}}
		err := csm.FindOneAndReplace(context.TODO(), filter, spmeas, opts).Err()
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}
		return nil
	}
	return nil
}
