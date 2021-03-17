package main

import (
	eh "github.com/Azure/azure-event-hubs-go/v3"
	azblob "github.com/Azure/azure-storage-blob-go/azblob"
)

func CreateEvent(blobItem azblob.BlobItemInternal, content []byte) *eh.Event {
	event := eh.Event{}
	event.Data = content
	event.Properties = make(map[string]interface{}, len(blobItem.Metadata))
	// TODO replace Property_ from Metadata
	for key, value := range blobItem.Metadata {
		event.Properties[key] = value
	}

	return &event
}
