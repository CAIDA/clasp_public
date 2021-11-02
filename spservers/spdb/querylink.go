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

func (cm *SpeedtestMongo) UpdateLinkstoMongo(region string, seents int64, linkmap map[string]*Link) {
	if cm != nil {
		//		basename := filepath.Base(LinkFile)
		//		namere := regexp.MustCompile(`(\S+)\.\d+\.links\.out`)
		//		namesplit := namere.FindStringSubmatch(LinkFile)
		//		region := namesplit[1]
		clink := cm.Database.Collection(Collinks)
		upfilter := bson.D{{"region", region}}
		disablecurrent := bson.D{{"$set", bson.D{{"current", false}}}}
		clink.UpdateMany(context.TODO(), upfilter, disablecurrent)
		for linkkey, link := range linkmap {
			filter := bson.D{{"region", region}, {"linkkey", linkkey}}
			lnkupdate := bson.D{{"$set",
				bson.D{
					{"region", region},
					{"linkkey", linkkey},
					{"nearip", link.NearIP},
					{"farip", link.FarIP},
					{"faras", link.FarAS},
					{"current", true},
					{"covered", false},
				},
			},
				{"$addToSet",
					bson.D{
						{"lastseen", seents},
					},
				}}
			/*			link.Region = region
						link.Linkkey = linkkey
						link.LastSeen = seents
						link.Current = true
						link.Covered = false*/
			opt := options.Update().SetUpsert(true)
			_, err := clink.UpdateOne(context.TODO(), filter, lnkupdate, opt)
			if err != nil {
				log.Panic(err)
			}
		}
	}
}

func (cm *SpeedtestMongo) QueryLinkbyId(linkid primitive.ObjectID) (*Link, error) {
	if cm.Database != nil {
		ldata := cm.Database.Collection(Collinks)
		linkfilter := bson.D{{"_id", linkid}}
		var linkobj Link
		err := ldata.FindOne(context.TODO(), linkfilter).Decode(&linkobj)
		if err != nil {
			return nil, err
		}
		return &linkobj, nil
	}
	return nil, errors.New("Database is nil")
}

func (cm *SpeedtestMongo) QueryLinkbyKey(region string, linkkey string) (*Link, error) {
	if cm.Database != nil {
		ldata := cm.Database.Collection(Collinks)
		linkfilter := bson.D{{"region", region}, {"linkkey", linkkey}}
		res := ldata.FindOne(context.TODO(), linkfilter)
		var err error
		var linkobj Link
		if res.Err() == mongo.ErrNoDocuments {
			return &Link{}, nil
		} else {
			err = res.Decode(&linkobj)
		}
		return &linkobj, err
	} else {
		return nil, errors.New("Database is nil")
	}
}

func (cm *SpeedtestMongo) QueryLinkbyFar(region string, farip string) ([]*Link, error) {
	var alllinks []*Link
	if cm.Database != nil {
		ldata := cm.Database.Collection(Collinks)
		monreg := regexp.MustCompile(`(\w+-\w+-)\w+`)
		regionarr := monreg.FindStringSubmatch(region)
		if len(regionarr) > 1 {
			linkfilter := bson.D{{"region", bson.D{{"$regex", regionarr[1] + "*"}}}, {"farip", farip}}
			cur, err := ldata.Find(context.TODO(), linkfilter)
			err = cur.All(context.TODO(), &alllinks)
			return alllinks, err
		}
		return nil, errors.New("Region format is incorrect")
	}
	return nil, errors.New("Database is nil")

}

func (cm *SpeedtestMongo) CreateLinkmap(region string) (map[string]*Link, map[string][]*Link, error) {
	var alllinks []*Link
	if cm.Database != nil {
		lnkdata := cm.Database.Collection(Collinks)
		linkfilter := bson.D{{"region", region}}
		cur, err := lnkdata.Find(context.TODO(), linkfilter)
		if cur != nil {
			err = cur.All(context.TODO(), &alllinks)
			if err != nil {
				return nil, nil, err
			}
			linkkeymap := make(map[string]*Link)
			faripmap := make(map[string][]*Link)
			for linkidx, link := range alllinks {
				linkkeymap[link.Linkkey] = alllinks[linkidx]
				if _, lexist := faripmap[link.FarIP]; !lexist {
					faripmap[link.FarIP] = make([]*Link, 0)
				}
				faripmap[link.FarIP] = append(faripmap[link.FarIP], alllinks[linkidx])
			}
			return linkkeymap, faripmap, nil
		} else {
			//no link found. empty map
			return make(map[string]*Link), make(map[string][]*Link), nil
		}
	}
	return nil, nil, errors.New("Database is nil")
}
