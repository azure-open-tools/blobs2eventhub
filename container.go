package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	eh "github.com/Azure/azure-event-hubs-go/v3"
	pipeline "github.com/Azure/azure-pipeline-go/pipeline"
	azblob "github.com/Azure/azure-storage-blob-go/azblob"
)

func parseContainer(azContainer azblob.ContainerItem, p pipeline.Pipeline, accountName string, containerFilter string, blobFilter string, c chan *[]*eh.Event, wg *sync.WaitGroup, marker azblob.Marker, metadataFilter []Filter) {
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
		events := parseBlobs(blobItems, blobFilter, containerServiceURL, metadataFilter)

		c <- events
	}
}
