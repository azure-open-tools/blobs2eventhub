package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"

	eh "github.com/Azure/azure-event-hubs-go/v3"
	azblob "github.com/Azure/azure-storage-blob-go/azblob"
)

func parseBlobs(blobItems []azblob.BlobItemInternal, blobFilter string, containerURL azblob.ContainerURL, metadataFilter []Filter) *[]*eh.Event {
	var blobWg sync.WaitGroup
	ec := make(chan *eh.Event)

	var events []*eh.Event

	for _, blobItem := range blobItems {
		if (len(blobFilter) == 0 || strings.Contains(blobItem.Name, blobFilter)) &&
			(len(metadataFilter) == 0 || containsMetadataMatch(blobItem.Metadata, metadataFilter)) {
			blobWg.Add(1)
			go createEvent(blobItem, &blobWg, ec, containerURL)
		}
	}

	go func() {
		blobWg.Wait()
		close(ec)
	}()

	for elem := range ec {
		events = append(events, elem)
	}
	return &events
}

func createEvent(blobItem azblob.BlobItemInternal, wg *sync.WaitGroup, c chan *eh.Event, containerURL azblob.ContainerURL) {
	defer wg.Done()

	content := downloadBlob(blobItem.Name, containerURL)
	event := CreateEvent(blobItem, content)

	c <- event
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
