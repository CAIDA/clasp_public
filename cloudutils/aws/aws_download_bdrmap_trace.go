package main

import (
	"cloudutils/common"
	"flag"
	"log"
	"mmbot"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type AwsDownloadConfig struct {
	DestDir      string
	BucketName   string
	BucketRegion string
	MaxKeySize   int64
	MMconfig     string
	MMclient     *mmbot.MMBot
	Archive      bool
}

var wg sync.WaitGroup

func handleListObjectError(mm *mmbot.MMBot, bucket string, err error) {
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				common.HandleError(mm, "No such bucket:"+bucket, aerr)
				//				log.Fatal(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				common.HandleError(mm, "List bucket error:"+bucket, aerr)
				//				log.Fatal(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			common.HandleError(mm, "List bucket error:"+bucket, err)
			//			log.Fatal(err.Error())
		}
	}
}

func ParseAwsDownloadConfig() *AwsDownloadConfig {
	awsdlcfg := &AwsDownloadConfig{}
	flag.StringVar(&awsdlcfg.DestDir, "d", "/scratch/cloudspeedtest/result", "Local directory for storing result files")
	flag.StringVar(&awsdlcfg.BucketName, "b", "cloudspeedtest", "Bucket name")
	flag.StringVar(&awsdlcfg.BucketRegion, "r", "us-west-1", "Region where the bucket located")
	flag.StringVar(&awsdlcfg.MMconfig, "mm", "/scratch/cloudspeedtest/bin/mattermostbot.json", "Path to mattermost config file")
	flag.Int64Var(&awsdlcfg.MaxKeySize, "s", 1000, "Number of results returned by API")
	flag.BoolVar(&awsdlcfg.Archive, "archive", true, "Move files into archive directory")
	flag.Parse()
	if awsdlcfg.MaxKeySize <= 0 {
		awsdlcfg.MaxKeySize = 1
	}
	awsdlcfg.MMclient = mmbot.NewMMBot(awsdlcfg.MMconfig)
	awsdlcfg.MMclient.Username = "AWS Downloader"
	if err := common.CreateDir(awsdlcfg.DestDir); err != nil {
		awsdlcfg.MMclient.SendPanic("Failed to create DestDir", awsdlcfg.DestDir)
		log.Fatal("Failed to create DestDir", awsdlcfg.DestDir)
	}
	return awsdlcfg
}

// DownloadFile download file with fileName, and move original file to archive
func downloadFile(awscfg *AwsDownloadConfig, fileName string, sess *session.Session, dataType string, month string, year string) {
	item := fileName
	defer wg.Done()

	// change filename to local file name and create directory structure
	parts := strings.Split(fileName, "/")
	fileName = filepath.Join(awscfg.DestDir, dataType, parts[0], year, month, filepath.Base(fileName))

	//	createDir(filepath.Dir(filepath.Join(awscfg.DestDir, dataType, parts[0], year, month, filepath.Base(fileName))))
	if errd := common.CreateDir(filepath.Dir(fileName)); errd != nil {
		common.HandleError(awscfg.MMclient, "Cannot create dir:"+filepath.Dir(fileName), errd)
	}

	// create local file for writing data into it
	file, err := os.Create(fileName)
	common.HandleError(awscfg.MMclient, "Failed to create file: "+fileName, err)
	defer file.Close()

	// fetch file and download
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(awscfg.BucketName),
			Key:    aws.String(item),
		})
	common.HandleError(awscfg.MMclient, "Failed download object: "+item, err)
	if awscfg.Archive {
		// move file to archive and delete file
		svc := s3.New(sess)
		copyFile(awscfg, awscfg.BucketName, awscfg.BucketName, item, filepath.Join("archive", item), svc, "ONEZONE_IA")
		deleteFile(awscfg, awscfg.BucketName, item, svc)
	}

	log.Println("Downloaded", file.Name(), numBytes, "bytes")
}

// getTopLevelFolders get top level folder of the input s3 bucket
func getTopLevelFolders(awscfg *AwsDownloadConfig, sess *session.Session) []string {
	// get folder names
	input := &s3.ListObjectsInput{
		Bucket:    aws.String(awscfg.BucketName),
		MaxKeys:   aws.Int64(awscfg.MaxKeySize), // max query size
		Prefix:    aws.String("aws"),
		Delimiter: aws.String("/"),
	}

	svc := s3.New(sess)
	result, err := svc.ListObjects(input)
	handleListObjectError(awscfg.MMclient, awscfg.BucketName, err)
	/*	if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					handleError(awscfg.MMclient, "No such bucket:"+awscfg.BucketName, aerr)
					//				log.Fatal(s3.ErrCodeNoSuchBucket, aerr.Error())
				default:
					handleError(awscfg.MMclient, "List bucket error:"+awscfg.BucketName, aerr)
					//				log.Fatal(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				handleError(awscfg.MMclient, "List bucket error:"+awscfg.BucketName, err)
				//			log.Fatal(err.Error())
			}
			return nil
		}
	*/
	// get folder names
	folders := make([]string, 0)
	for _, v := range result.CommonPrefixes {
		folders = append(folders, (*v.Prefix))
	}

	return folders
}

// copyFile move file from one bucket to the other
func copyFile(awscfg *AwsDownloadConfig, frombucket string, tobucket string, item string, dest string, svc *s3.S3, storageClass string) {
	// Copy the item
	_, err := svc.CopyObject(
		&s3.CopyObjectInput{
			Bucket:       aws.String(tobucket),
			CopySource:   aws.String(filepath.Join(frombucket, item)),
			Key:          aws.String(dest), // The key of the destination object.
			StorageClass: aws.String(storageClass),
		})

	if err != nil {
		errmsg := fmt.Sprintf("Unable to copy item from bucket %q to bucket %q", frombucket, tobucket)
		common.HandleError(awscfg.MMclient, errmsg, err)
	}

	// Wait to see if the item got copied
	err = svc.WaitUntilObjectExists(
		&s3.HeadObjectInput{
			Bucket: aws.String(tobucket),
			Key:    aws.String(item),
		})

	if err != nil {
		errmsg := fmt.Sprintf("Error occurred while waiting for item %q to be copied to bucket %q, %v", item, tobucket, dest)
		common.HandleError(awscfg.MMclient, errmsg, err)
		log.Fatal(errmsg, err)
	}

	log.Printf("Item %q successfully copied from bucket %q to bucket %q\n", item, tobucket, dest)
}

// deleteFile: delete the file
func deleteFile(awscfg *AwsDownloadConfig, srcbucket string, key string, svc *s3.S3) {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(srcbucket),
		Key:    aws.String(key),
	}

	result, err := svc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				common.HandleError(awscfg.MMclient, "Delete error:"+key, aerr)
				log.Fatal(aerr.Error())
			}
		} else {
			common.HandleError(awscfg.MMclient, "Delete error:"+key, aerr)
			log.Fatal(err.Error())
		}
		return
	}

	log.Println(result)
}

// DownloadBdrmap download latest bdrmap data from bucket
func downloadBdrmap(awscfg *AwsDownloadConfig) {
	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(awscfg.BucketRegion),
	})

	// get folder names
	folders := getTopLevelFolders(awscfg, sess)
	if folders == nil || len(folders) == 0 {
		log.Println("Empty top dir")
		return
	}
	// iterate over files in each folder and download latest bdrmap
	for _, prefix := range folders {
		continueToken := ""

		for {
			input := &s3.ListObjectsV2Input{
				Bucket:  aws.String(awscfg.BucketName),
				MaxKeys: aws.Int64(awscfg.MaxKeySize), // max query size
				Prefix:  aws.String(filepath.Join(prefix, "bdrmap")),
			}

			if continueToken != "" {
				input.SetContinuationToken(continueToken)
			}

			svc := s3.New(sess)

			// make sure download the latest
			result, err := svc.ListObjectsV2(input)
			handleListObjectError(awscfg.MMclient, awscfg.BucketName, err)
			/*
				if err != nil {
					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						case s3.ErrCodeNoSuchBucket:
							log.Fatal(s3.ErrCodeNoSuchBucket, aerr.Error())
						default:
							log.Fatal(aerr.Error())
						}
					} else {
						log.Fatal(err.Error())
					}
					return
				}
			*/
			for i := 0; i < len(result.Contents); i++ {
				wg.Add(1)
				go downloadFile(awscfg, *(result.Contents[i].Key), sess, "bdrmap", "", "")
			}

			if !(*result.IsTruncated) {
				break
			}
			continueToken = *(result.NextContinuationToken)

		}
	}
	wg.Wait()
	msg := []string{"Downloaded bdrmap files from"}
	msg = append(msg, folders...)
	awscfg.MMclient.SendInfo(msg...)
}

// DownloadBdrmap download latest bdrmap data from bucket
func downloadTrace(awscfg *AwsDownloadConfig) {
	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(awscfg.BucketRegion),
	})

	// get folder names
	folders := getTopLevelFolders(awscfg, sess)
	if folders == nil || len(folders) == 0 {
		log.Println("Empty top dir")
		return
	}

	// iterate over files in each folder and download latest bdrmap
	for _, prefix := range folders {
		// partition till reach the end
		continueToken := ""
		for {
			input := &s3.ListObjectsV2Input{
				Bucket:  aws.String(awscfg.BucketName),
				MaxKeys: aws.Int64(awscfg.MaxKeySize), // max query size
				Prefix:  aws.String(prefix + "trace"),
			}

			svc := s3.New(sess)

			if continueToken != "" {
				input.SetContinuationToken(continueToken)
			}

			// make sure download the latest
			result, err := svc.ListObjectsV2(input)
			handleListObjectError(awscfg.MMclient, awscfg.BucketName, err)
			/*
				if err != nil {
					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						case s3.ErrCodeNoSuchBucket:
							fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
						default:
							fmt.Println(aerr.Error())
						}
					} else {
						// Print the error, cast err to awserr.Error to get the Code and
						// Message from an error.
						fmt.Println(err.Error())
					}
					return
				}*/

			// each time do 100 threads (1000 may be too many)
			for i := 0; i < len(result.Contents); i++ {
				wg.Add(1)
				timeStamp := result.Contents[i].LastModified
				go downloadFile(awscfg, *(result.Contents[i].Key), sess, "trace", strconv.Itoa(int(timeStamp.Month())), strconv.Itoa(timeStamp.Year()))
			}

			if !(*result.IsTruncated) {
				break
			}
			continueToken = *(result.NextContinuationToken)
			wg.Wait()
		}
	}
	msg := []string{"Downloaded traceroute files from "}
	msg = append(msg, folders...)
	awscfg.MMclient.SendInfo(msg...)
}

func main() {
	awscfg := ParseAwsDownloadConfig()
	downloadBdrmap(awscfg)
	downloadTrace(awscfg)
}
