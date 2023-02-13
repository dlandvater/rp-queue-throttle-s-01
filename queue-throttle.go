package main

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	err        error
	token      string
	MsgId      string
	ctx        context.Context
	dataClient *spanner.Client
)

type ListSku struct {
	RowId       string
	OrgId       string
	ItemId      string
	LocationId  string
	TriggerTime time.Time
}
type MessageSku struct {
	OrgId      string
	ItemId     string
	LocationId string
}
type QueueTriggerRoute struct {
	OrgId       string
	RouteId     string
	TriggerTime time.Time
}
type MessageRoute struct {
	OrgId   string
	RouteId string
}

func main() {

	fmt.Println("start of main")

	token = os.Getenv("PUBSUB_VERIFICATION_TOKEN") // token is used to verify push requests.

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		log.Println("NewClient error", err)
		insertErrorLog("0", "", "", err, 1)
	}
	defer client.Close()

	http.HandleFunc("/pubsub/push", pushHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}
	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func pushHandler(w http.ResponseWriter, r *http.Request) {

	var err error
	ctx = context.Background()

	verified := verifyMessage(w, r)
	if verified {
		// Client
		//dataClient, err = spanner.NewClient(ctx, "projects/rp-database-s-01/instances/rp-combined/databases/retail")
		// Emulator client
		dataClient, err = spanner.NewClient(ctx, "projects/rp-database-s-01/instances/rp-combined/databases/retail")
		if err != nil {
			log.Println("new spanner client error", err)
			insertErrorLog("0", "", "", err, 1)
		}
		defer dataClient.Close()

		messagesMu.Lock()
		defer messagesMu.Unlock()

		//TODO calculate number of rows to pull from each queue

		queueToMessageSku("forecast", 200, 10)
		//TODO remove testing
		//queueToMessageSku("supply", 150, 10)
		//queueToMessageSku("on_hand_on_order", 400, 10)
		//queueToMessageSku("sales_history", 500, 10)
	} else {
		log.Println("Invalid token")
	}
}
