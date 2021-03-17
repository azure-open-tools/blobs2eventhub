package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"

	eh "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-storage-blob-go/azblob"

	sender "github.com/azure-open-tools/event-hubs/sender"
	"github.com/spf13/cobra"
)

type arguments struct {
	AccountName              string
	AccessKey                string
	ContainerName            string
	BlobName                 string
	EventhubConnectionString string
	MetadataFilter           []string
}

var largs = arguments{}

var rootCmd = &cobra.Command{
	Use:   "blobs2eventhub",
	Short: "blobs2eventhub sends blobs to an eventhub",
	Long: `blobs2eventhub sends blobs to an eventhub.
Complete documentation is available at http://github.com/azure-open-tools/blobs2eventhub`,
	Run: func(cmd *cobra.Command, args []string) {
		exec(largs)
	},
	Version: getVersion(),
}

const (
	storageURLTemplate   = "https://%s.blob.core.windows.net"
	containerURLTemplate = "https://%s.blob.core.windows.net/%s"
)

func init() {
	rootCmd.Flags().StringVarP(&largs.AccountName, "accountName", "n", "", "accountName of the Storage Account")
	rootCmd.Flags().StringVarP(&largs.AccessKey, "accessKey", "k", "", "accessKey for the Storage Account")
	rootCmd.Flags().StringVarP(&largs.ContainerName, "container", "c", "", "filter for container name with substring match")
	rootCmd.Flags().StringVarP(&largs.BlobName, "blob", "b", "", "filter for blob name with substring match")
	rootCmd.Flags().StringVarP(&largs.EventhubConnectionString, "ehConnString", "e", "", "Connection String of the Eventhub")
	rootCmd.Flags().StringSliceVarP(&largs.MetadataFilter, "metadata-filter", "m", []string{}, "OR filter for blob metadata. Structure is <key>:<value>")
	rootCmd.MarkFlagRequired("accountName")
	rootCmd.MarkFlagRequired("accessKey")
	rootCmd.MarkFlagRequired("ehConnString")
	rootCmd.SetVersionTemplate(getVersion())
}

func getConnString(connString string) string {
	if len(strings.TrimSpace(connString)) > 0 {
		return connString
	} else {
		return os.Getenv("EVENTHUB_SEND_CONNSTR")
	}
}

func exec(args arguments) {
	ctx := context.Background()

	builder := sender.NewSenderBuilder()
	builder.SetConnectionString(getConnString(args.EventhubConnectionString))
	snd, err := builder.GetSender()

	if err != nil {
		log.Fatal("Error creating eventhub sender")
	}

	// Create a default request pipeline using your storage account name and account key
	credential, authErr := azblob.NewSharedKeyCredential(args.AccountName, args.AccessKey)
	if authErr != nil {
		log.Fatal("Error while Authentication")
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint
	URL, _ := url.Parse(fmt.Sprintf(storageURLTemplate, args.AccountName))

	serviceURL := azblob.NewServiceURL(*URL, p)

	metadataFilter := createMetadataFilter(args.MetadataFilter)

	c := make(chan *[]*eh.Event)
	var wg sync.WaitGroup
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listContainer, err := serviceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})

		if err != nil {
			log.Fatal("Error while getting Container")
		}

		for _, val := range listContainer.ContainerItems {
			wg.Add(1)
			go parseContainer(val, p, args.AccountName, args.ContainerName, args.BlobName, c, &wg, marker, metadataFilter)
		}
		// used for Pagination
		marker = listContainer.NextMarker
	}

	// wait for all entries in waitgroup and close then the channel
	go func() {
		wg.Wait()
		close(c)
	}()

	// channel to collect results
	for elem := range c {
		snd.SendEventsAsBatch(context.Background(), elem)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rootCmd.Execute()
}
