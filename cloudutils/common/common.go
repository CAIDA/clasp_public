package common

import (
	"log"
	"mmbot"
	"os"
)

func HandleError(mm *mmbot.MMBot, desc string, e error) {
	if e != nil {
		mm.SendPanic(desc, e.Error())
		log.Fatal(e)
	}
}

// createDir create directory in the target path
func CreateDir(dirName string) error {
	_, err := os.Stat(dirName)

	if os.IsNotExist(err) {
		errDir := os.MkdirAll(dirName, 0755)
		if errDir != nil {
			return err
		}
	}
	return nil
}
