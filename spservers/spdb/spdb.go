package spdb

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	speedtestmongo = "mongodb://localhost:27017"
	Colserver      = "speedserver"
	Collinks       = "links"
	Coldatastatus  = "datastatus"
	Coltraceroute  = "traceroute"
	Colspeedmeas   = "speedmeas"
)

type SpeedtestMongo struct {
	config   *DBConfig
	Client   *mongo.Client
	Database *mongo.Database
}

type DBConfig struct {
	Username string
	Password string
	AuthDB   string
	DB       string
}

func connectmongo(mongopath, dbname string) (*mongo.Client, *mongo.Database) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongopath))
	if err != nil {
		log.Panic(err)
		return nil, nil
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Panic(err)
		return nil, nil
	}
	log.Println("Database connected")
	return client, client.Database(dbname)
}

func (cm *SpeedtestMongo) Close() {
	if cm.Client != nil {
		err := cm.Client.Disconnect(context.TODO())
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Database connection ends")
	} else {
		log.Println("Mongo clientt is nil")
	}
}

func NewMongoDB(config string, dbname string) *SpeedtestMongo {
	cfile, err := os.Open(config)
	defer cfile.Close()
	if err != nil {
		log.Panic(err)
		return nil
	}
	c := &SpeedtestMongo{}
	decoder := json.NewDecoder(cfile)
	c.config = &DBConfig{}
	err = decoder.Decode(c.config)
	if err != nil {
		log.Panic(err)
		return nil
	}
	mongostr := "mongodb://"
	if c.config.DB == "" {
		log.Fatal("no database was selected")
	}
	if c.config.Username != "" && c.config.Password != "" {
		mongostr = mongostr + c.config.Username + ":" + c.config.Password + "@" + c.config.DB
		if c.config.AuthDB != "" {
			mongostr = mongostr + "/?authSource=" + c.config.AuthDB
		}
	}
	c.Client, c.Database = connectmongo(mongostr, dbname)
	return c
}
