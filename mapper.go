package main

import (
	eh "github.com/Azure/azure-event-hubs-go/v3"
)

func CreateEvents(blobs []Blob) []eh.Event {
	var events []eh.Event
	for _, b := range blobs {
		event := new(eh.Event)
		event.Data = b.Content
		// TODO replace Property_ from Metadata
		event.Properties = b.Metadata
		events = append(events, *event)
	}
	return events
}
