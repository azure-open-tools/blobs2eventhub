package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	pipeline "github.com/Azure/azure-pipeline-go/pipeline"
	azblob "github.com/Azure/azure-storage-blob-go/azblob"
)

func getConnString(connString string) string {
	if len(strings.TrimSpace(connString)) > 0 {
		return connString
	} else {
		return os.Getenv("EVENTHUB_SEND_CONNSTR")
	}
}

func parseContainer(azContainer azblob.ContainerItem, p pipeline.Pipeline, accountName string, containerFilter string, blobFilter string, c chan *[]eh.Event, wg *sync.WaitGroup, marker azblob.Marker, metadataFilter []Filter) {
	defer wg.Done()
	containerName := azContainer.Name

	// TODO substring match? to match containers: ['test-1', 'test-2'], term: 'test, matches ['test-1', 'test-2']
	if len(containerFilter) > 0 && !strings.Contains(containerName, containerFilter) {
		return
	}

	containerURL, _ := url.Parse(fmt.Sprintf(containerURLTemplate, accountName, containerName))
	containerServiceURL := azblob.NewContainerURL(*containerURL, p)

	ctx := context.Background()

	for blobMarker := marker; blobMarker.NotDone(); {
		listBlob, _ := containerServiceURL.ListBlobsFlatSegment(ctx, blobMarker, azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}})
		blobMarker = listBlob.NextMarker
		blobItems := listBlob.Segment.BlobItems
		foundBlobs := parseBlobs(blobItems, blobFilter, containerServiceURL, metadataFilter)
		events := CreateEvents(foundBlobs)
		// send to channel
	}
}
