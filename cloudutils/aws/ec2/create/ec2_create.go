package main

import (
	"cloudutils/aws/ec2/ec2utils"
	"encoding/json"
	"flag"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/session"
)

// check if user provide DB config
func main() {
	// parse arguments
	vmConfigPathPtr := flag.String("v", "None", "required: path to vm config json file")
	dbConfigPathPtr := flag.String("d", "None", "optional: path to db config file")
	flag.Parse()

	// download json data
	body, err := ioutil.ReadFile(*vmConfigPathPtr)
	ec2utils.HandleError(err)

	// parse the json into structs
	var config ec2utils.CreateVMInput
	json.Unmarshal([]byte(body), &config)

	// region will be user's own aws config default region
	sess, err := session.NewSession()
	ec2utils.HandleError(err)

	ec2utils.CreateEC2Instance(sess, config, *dbConfigPathPtr)
}
