package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"

	azblob "github.com/Azure/azure-storage-blob-go/azblob"
	sender "github.com/azure-open-tools/event-hubs/sender"
)

type blob struct {
	Name       string            `json:"name"`
	Content    []byte            `json:"content"`
	Properties map[string]string `json:"properties"`
	Metadata   map[string]string `json:"metadata"`
}

func parseBlobs(blobItems []azblob.BlobItemInternal, blobFilter string, containerURL azblob.ContainerURL, metadataFilter []Filter) []blob {
	var blobWg sync.WaitGroup
	bc := make(chan *blob)

	var blobs []blob

	for _, blobItem := range blobItems {
		if (len(blobFilter) > 0 && strings.Contains(blobItem.Name, blobFilter)) &&
			(len(metadataFilter) == 0 || (len(metadataFilter) > 0 && containsMetadataMatch(blobItem.Metadata, metadataFilter))) {
			blobWg.Add(1)
			go createBlob(blobItem, &blobWg, bc, containerURL)
		}
	}

	go func() {
		blobWg.Wait()
		close(bc)
	}()

	for elem := range bc {
		blobs = append(blobs, *elem)
	}
	return blobs
}

func createBlob(blobItem azblob.BlobItemInternal, wg *sync.WaitGroup, c chan *blob, containerURL azblob.ContainerURL) {
	defer wg.Done()

	blob := new(blob)
	blob.Name = blobItem.Name
	blob.Metadata = blobItem.Metadata
	blob.Content = downloadBlob(blobItem.Name, containerURL)

	builder := sender.NewSenderBuilder()
	builder.SetConnectionString(getConnString(""))
	//builder.AddProperties()
	snd, err := builder.GetSender()
	snd.AddProperties(blob.Metadata)

	c <- blob
}

func downloadBlob(blobName string, containerUrl azblob.ContainerURL) []byte {
	blobURL := containerUrl.NewBlockBlobURL(blobName)
	downloadResponse, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})

	if err != nil {
		log.Fatalf("Error downloading blob %s", blobName)
	}

	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	downloadedData := bytes.Buffer{}
	_, err = downloadedData.ReadFrom(bodyStream)

	if err != nil {
		log.Fatalf("Error reading blob %s", blobName)
	}

	return downloadedData.Bytes()
}
