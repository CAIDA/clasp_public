package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

var wg1 sync.WaitGroup
var wg2 sync.WaitGroup

// createDir create directory in the target path
func createDir(dirName string) {
	_, err := os.Stat(dirName)

	if os.IsNotExist(err) {
		errDir := os.MkdirAll(dirName, 0755)
		if errDir != nil {
			log.Fatal(err)
		}
	}
}

func handleError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func createFile(fileName string) *os.File {
	f, err := os.Create(fileName)
	defer f.Close()
	handleError(err)
	return f
}

func downloadFile(ctx context.Context, blobURL azblob.BlockBlobURL, fileName string, dirPrefix string) {
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	handleError(err)
	defer wg2.Done()

	// NOTE: automatically retries are performed if the connection fails
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	// read the body into a buffer
	downloadedData := bytes.Buffer{}
	_, err = downloadedData.ReadFrom(bodyStream)

	// create local file and write to it
	filePath := filepath.Join(dirPrefix, fileName)
	file := createFile(filePath)
	err = ioutil.WriteFile(filePath, downloadedData.Bytes(), 0644)
	handleError(err)

	// success
	log.Println("Downloaded", file.Name(), len(downloadedData.Bytes()), "bytes")
}

func getContainerURL(containerName string) azblob.ContainerURL {
	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Fatal("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)
	return containerURL
}

func getDirectoryPrefix(ctx context.Context, prefix string, deliminter string, containerURL azblob.ContainerURL) []string {
	res := []string{}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsHierarchySegment(ctx, marker, deliminter, azblob.ListBlobsSegmentOptions{Prefix: prefix})
		handleError(err)

		// ListBlobs returns the start of the next segment
		marker = listBlob.NextMarker

		// Process the dir prefix
		for _, dirInfo := range listBlob.Segment.BlobPrefixes {
			fmt.Print("	dir name: " + dirInfo.Name + "\n")
			res = append(res, dirInfo.Name)
		}
	}

	return res
}

func downloadTrace(containerURL azblob.ContainerURL, prefix string) {
	defer wg1.Done()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		ctx := context.Background()
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: prefix + "trace"})
		handleError(err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			log.Print("	Blob name: " + blobInfo.Name + "\n")
			ctx := context.Background()
			blobURL := containerURL.NewBlockBlobURL(blobInfo.Name)

			// store based on year/month
			timeStamp := blobInfo.Properties.LastModified
			month := strconv.Itoa(int(timeStamp.Month()))
			year := strconv.Itoa(int(timeStamp.Year()))
			parts := strings.Split(prefix, "/")
			fileName := filepath.Base(blobInfo.Name)
			newPrefix := filepath.Join("/scratch/cloudspeedtest/result/trace", parts[0], year, month)

			// copy file to archive
			archivePath := filepath.Join("archive", blobInfo.Name)
			newURL := containerURL.NewBlobURL(archivePath)
			originalURL := containerURL.NewBlobURL(blobInfo.Name)
			copyFileToArchive(ctx, newURL, originalURL)
			setAccessTier(ctx, newURL, "Cool")

			// create directory
			createDir(newPrefix)

			// download file
			wg2.Add(1)
			go func() {
				downloadFile(ctx, blobURL, fileName, newPrefix)
				deleteBlob(ctx, originalURL)
			}()
		}
		wg2.Wait()
	}
}

func downloadBdrmap(containerURL azblob.ContainerURL, prefix string) {
	defer wg1.Done()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		ctx := context.Background()
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: prefix + "bdrmap"})
		handleError(err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// only return the latest bdrmap
		resLength := len(listBlob.Segment.BlobItems)
		if resLength > 0 {
			blobInfo := listBlob.Segment.BlobItems[resLength-1]
			ctx := context.Background()
			blobURL := containerURL.NewBlockBlobURL(blobInfo.Name)

			// store based on year/month
			parts := strings.Split(prefix, "/")
			fileName := filepath.Base(blobInfo.Name)
			newPrefix := filepath.Join("/scratch/cloudspeedtest/result/bdrmap", parts[0])
			fmt.Println(newPrefix)

			// copy file to archive and set access tier
			archivePath := filepath.Join("archive", blobInfo.Name)
			newURL := containerURL.NewBlobURL(archivePath)
			originalURL := containerURL.NewBlobURL(blobInfo.Name)
			copyFileToArchive(ctx, newURL, originalURL)
			setAccessTier(ctx, newURL, "Cool")

			//create directory
			createDir(newPrefix)

			//download file
			wg2.Add(1)
			go func() {
				downloadFile(ctx, blobURL, fileName, newPrefix)
				deleteBlob(ctx, originalURL)
			}()
		}
	}
	wg2.Wait()
}

func copyFileToArchive(ctx context.Context, archiveURL azblob.BlobURL, srcURL azblob.BlobURL) {
	_, err := archiveURL.StartCopyFromURL(ctx, srcURL.URL(), azblob.Metadata{},
		azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{})
	handleError(err)
	log.Println("successfully copied from", srcURL, "to", archiveURL)
}

func deleteBlob(ctx context.Context, blobURL azblob.BlobURL) {
	_, err := blobURL.Delete(ctx, "", azblob.BlobAccessConditions{})
	handleError(err)
	log.Println("successfully deleted", blobURL)
}

func setAccessTier(ctx context.Context, blobURL azblob.BlobURL, tier azblob.AccessTierType) {
	_, err := blobURL.SetTier(ctx, tier, azblob.LeaseAccessConditions{})
	handleError(err)
	log.Println("successfully set tier for", blobURL)
}

func main() {
	ctx := context.Background()
	containerURL := getContainerURL("cloudspeedtestcontainer")

	// download traces
	dirPrefix := getDirectoryPrefix(ctx, "", "/", containerURL)

	for _, prefix := range dirPrefix {
		wg1.Add(1)
		go downloadTrace(containerURL, prefix)
	}
	wg1.Wait()

	// download bdrmap
	for _, prefix := range dirPrefix {
		wg1.Add(1)
		go downloadBdrmap(containerURL, prefix)
	}
	wg1.Wait()
}
