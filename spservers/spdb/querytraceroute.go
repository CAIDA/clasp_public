package spdb

import (
	"context"
	"errors"
	"log"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	//	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LinkSpAgg struct {
	Groupid struct {
		Linkid primitive.ObjectID `bson:"linkid"`
	} `bson: "_id"`
	SpServerIds []primitive.ObjectID `bson:"spservers"`
	TrIds       []primitive.ObjectID `bson:"tr"`
	LinkObj     []Link               `bson:"links"`
}

func (cm *SpeedtestMongo) InsertManyTraceroutes(trs []*Traceroute) (int, error) {
	if cm.Database != nil {
		if len(trs) > 0 {
			ctr := cm.Database.Collection(Coltraceroute)
			//insert others even on failed
			opts := options.InsertMany().SetOrdered(false)
			islice := make([]interface{}, len(trs))
			for tridx, _ := range trs {
				islice[tridx] = trs[tridx]
			}
			res, err := ctr.InsertMany(context.TODO(), islice, opts)
			if err != nil {
				log.Println(err)
				return 0, err
			}
			return len(res.InsertedIDs), nil
		}
	}
	return -1, errors.New("Database is nil")
}

func (cm *SpeedtestMongo) QueryLinksSpServerMatch(region string, startts int64, outputch chan *LinkSpAgg) error {
	if cm.Database != nil {
		ctr := cm.Database.Collection(Coltraceroute)
		pipeline := bson.A{
			bson.D{{"$match", bson.D{
				{"$and", bson.A{
					bson.D{{"region", region}},
					bson.D{{"linkid", bson.D{{"$ne", primitive.NilObjectID}}}},
					bson.D{{"ts", bson.D{{"$gte", startts}}}},
				}},
			}}},
			bson.D{{"$group", bson.D{
				{"_id", bson.D{
					{"linkid", "$linkid"},
				}},
				{"spservers", bson.D{
					{"$push", "$spserverid"},
				}},
				{"tr", bson.D{
					{"$push", "$_id"},
				}},
			}}},
			bson.D{{"$lookup", bson.D{
				{"from", "links"},
				{"localField", "_id.linkid"},
				{"foreignField", "_id"},
				{"as", "links"},
			}}},
		}
		cursor, err := ctr.Aggregate(context.TODO(), pipeline)
		if err != nil {
			log.Println(err)
			return err
		}
		defer cursor.Close(context.TODO())
		for cursor.Next(context.TODO()) {
			result := &LinkSpAgg{}
			if err := cursor.Decode(result); err != nil {
				log.Println("Decode error", err)
				return err
			}
			outputch <- result
		}
		return nil
	}
	return errors.New("Database is nil")
}

func (cm *SpeedtestMongo) ListRegions() ([]string, error) {
	if cm.Database != nil {
		ctr := cm.Database.Collection(Coltraceroute)
		filter := bson.D{}
		values, err := ctr.Distinct(context.TODO(), "region", filter)
		if err != nil {
			log.Println("List region error", err)
			return nil, err
		}
		rg := make([]string, len(values))
		for idx, value := range values {
			rg[idx] = value.(string)
		}
		return rg, nil
	}
	return nil, errors.New("Database is nil")
}

func (cm *SpeedtestMongo) TracerouteDestRtt(trids []primitive.ObjectID) (map[string][]float64, map[string][]primitive.ObjectID, error) {
	if cm.Database != nil {
		ctr := cm.Database.Collection(Coltraceroute)
		rttmap := make(map[string][]float64)
		tracemapid := make(map[string][]primitive.ObjectID)
		for _, trid := range trids {
			var trdata Traceroute
			filter := bson.D{{"_id", trid}}
			err := ctr.FindOne(context.TODO(), filter).Decode(&trdata)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					continue
				}
				log.Println("traceroute rtt error", err)
				return nil, nil, err
			}
			if _, sexist := tracemapid[trdata.SpServerId.Hex()]; !sexist {
				tracemapid[trdata.SpServerId.Hex()] = []primitive.ObjectID{trid}
			} else {
				tracemapid[trdata.SpServerId.Hex()] = append(tracemapid[trdata.SpServerId.Hex()], trid)
			}
			if len(trdata.Hops) > 1 {
				sort.Slice(trdata.Hops, func(i, j int) bool {
					return trdata.Hops[j].ProbeTTL < trdata.Hops[i].ProbeTTL
				})
				trspid := trdata.SpServerId.Hex()
				if _, rexist := rttmap[trspid]; !rexist {
					rttmap[trspid] = []float64{trdata.Hops[0].Rtt}
				} else {
					rttmap[trspid] = append(rttmap[trspid], trdata.Hops[0].Rtt)
				}
			}
		}
		return rttmap, tracemapid, nil
	}
	return nil, nil, errors.New("Database is nil")
}

func (cm *SpeedtestMongo) TracerouteASPathLen(trids []primitive.ObjectID) ([]int, error) {
	if cm.Database != nil {
		ctr := cm.Database.Collection(Coltraceroute)
		aslen := make([]int, len(trids))
		for tridx, trid := range trids {
			var trdata Traceroute
			filter := bson.D{{"_id", trid}}
			err := ctr.FindOne(context.TODO(), filter).Decode(&trdata)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					continue
				}
				log.Println("traceroute not found", trid, err)
				return nil, err
			}
			asseen := make([]string, 0)
			for _, hop := range trdata.Hops {
				if hop.Asn != "" {
					insertidx := sort.SearchStrings(asseen, hop.Asn)
					if insertidx == len(asseen) {
						//does not exist, append to the end
						asseen = append(asseen, hop.Asn)
					} else {
						//does not exist, insert
						if asseen[insertidx] != hop.Asn {
							newasseen := make([]string, len(asseen)+1)
							copy(newasseen, asseen[:insertidx])
							newasseen[insertidx] = hop.Asn
							copy(newasseen[insertidx+1:], asseen[insertidx:])
							asseen = newasseen
						}
					}
				}
			}
			aslen[tridx] = len(asseen)
		}
		return aslen, nil
	}
	return nil, errors.New("Database is nil")
}

func (cm *SpeedtestMongo) SpServersLinkChoice(region, spidhex string) (int, error) {
	if cm.Database != nil {
		ctr := cm.Database.Collection(Coltraceroute)
		spid, _ := primitive.ObjectIDFromHex(spidhex)
		filter := bson.D{{"region", region}, {"spserverid", spid}}
		values, err := ctr.Distinct(context.TODO(), "linkid", filter)
		return len(values), err
	}
	return 0, errors.New("Database is nil")
}
