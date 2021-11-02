package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

var wg1 sync.WaitGroup

func handleError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func createDir(dirName string) {
	_, err := os.Stat(dirName)

	if os.IsNotExist(err) {
		errDir := os.MkdirAll(dirName, 0755)
		if errDir != nil {
			log.Fatal(err)
		}
	}
}

func createFile(fileName string) *os.File {
	f, err := os.Create(fileName)
	defer f.Close()
	handleError(err)
	return f
}

// init create client access point to stroage
func initClient() (context.Context, *storage.Client) {
	// by default its using GOOGLE_APPLICATION_CREDENTIALS env var as authentication
	ctx := context.Background()
	client, err := storage.NewClient(context.Background())
	handleError(err)
	return ctx, client
}

// getTopLevelDir get top level directory
func getTopLevelDir(ctx context.Context, bucket *storage.BucketHandle) []string {
	query := &storage.Query{Prefix: "", Delimiter: "/"}

	var dirNames []string
	it := bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		handleError(err)
		dirNames = append(dirNames, attrs.Prefix)
	}

	return dirNames
}

// moveFile moves an object into another location.
func moveFile(ctx context.Context, client *storage.Client, bucket, object string, archivePath string) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	dstName := archivePath
	src := client.Bucket(bucket).Object(object)
	dst := client.Bucket(bucket).Object(dstName)
	copier := dst.CopierFrom(src)
	copier.ObjectAttrs = storage.ObjectAttrs{StorageClass: "COLDLINE"}

	if _, err := copier.Run(ctx); err != nil {
		log.Fatalf("Object(%q).CopierFrom(%q).Run: %v", dstName, object, err)
	}
	if err := src.Delete(ctx); err != nil {
		log.Fatalf("Object(%q).Delete: %v", object, err)
	}
	log.Printf("Blob %v moved to %v.\n", object, dstName)
}

// downloadFile download file from bucket, and move downloaded file to archive
func downloadFile(ctx context.Context, bucketName string, client *storage.Client, fileName string, dirPrefix string) {
	rc, err := client.Bucket(bucketName).Object(fileName).NewReader(ctx)
	handleError(err)
	defer rc.Close()
	defer wg1.Done()

	data, err := ioutil.ReadAll(rc)
	handleError(err)

	// create local file and write to it
	fileBaseName := filepath.Base(fileName)
	filePath := filepath.Join(dirPrefix, fileBaseName)
	file := createFile(filePath)
	err = ioutil.WriteFile(filePath, data, 0644)
	handleError(err)

	// copy file to archive and set access tier
	archivePath := filepath.Join("archive", fileName)
	moveFile(ctx, client, bucketName, fileName, archivePath)

	// success
	log.Println("Downloaded", file.Name(), len(data), "bytes")
}

// download bdrmap
func downloadBdrmap(ctx context.Context, bucket *storage.BucketHandle, bucketName string, client *storage.Client) {
	dirNames := getTopLevelDir(ctx, bucket)

	for _, dirName := range dirNames {
		query := &storage.Query{Prefix: dirName + "results/bdrmap", Delimiter: ""}
		it := bucket.Objects(ctx, query)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			handleError(err)

			// case 1: reach the latest bdrmap, download it
			parts := strings.Split(attrs.Name, "/")
			newPrefix := filepath.Join("/scratch/cloudspeedtest/result/bdrmap", parts[0])

			// download file
			createDir(newPrefix)
			wg1.Add(1)
			go downloadFile(context.Background(), bucketName, client, attrs.Name, newPrefix)
		}
		wg1.Wait()
	}
}

func downloadTrace(ctx context.Context, bucket *storage.BucketHandle, bucketName string, client *storage.Client) {
	dirNames := getTopLevelDir(ctx, bucket)

	for _, dirName := range dirNames {
		query := &storage.Query{Prefix: dirName + "results/trace", Delimiter: ""}
		it := bucket.Objects(ctx, query)

		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			handleError(err)

			// store based on month/year
			timeStamp := attrs.Created
			month := strconv.Itoa(int(timeStamp.Month()))
			year := strconv.Itoa(int(timeStamp.Year()))
			parts := strings.Split(attrs.Name, "/")
			newPrefix := filepath.Join("/scratch/cloudspeedtest/result/trace", parts[0], year, month)
			fmt.Println(newPrefix)

			// download file
			createDir(newPrefix)
			wg1.Add(1)
			go downloadFile(ctx, bucketName, client, attrs.Name, newPrefix)
		}
		wg1.Wait()
	}
}

func main() {
	// create client
	ctx, client := initClient()
	defer client.Close()
	bucket := client.Bucket("cloudspeedtest")
	dirNames := getTopLevelDir(ctx, bucket)
	fmt.Println(dirNames)

	downloadBdrmap(ctx, bucket, "cloudspeedtest", client)
	downloadTrace(ctx, bucket, "cloudspeedtest", client)
}
