package main

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"context"
	"database/sql"
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

type QueueFcstSuply struct {
	OrgId       string
	ItemId      string
	LocationId  string
	TriggerTime time.Time
}
type MessageFcstSuply struct {
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

type ListOnHandOnOrder struct {
	OrgId      string
	ItemId     string
	LocationId string
}
type QueueOnHandOnOrder struct {
	TypeId        string
	OrgId         string
	ItemId        string
	LocationId    string
	OnHand        sql.NullFloat64
	DueDate       sql.NullTime
	OrderQuantity sql.NullFloat64
	OrderId       sql.NullString
	Picked        sql.NullString
	ShipDate      sql.NullTime
	Supplier      sql.NullString
	TriggerTime   time.Time
}
type MessageOnHandOnOrder struct {
	TypeId        string
	OrgId         string
	ItemId        string
	LocationId    string
	OnHand        sql.NullFloat64
	DueDate       sql.NullTime
	OrderQuantity sql.NullFloat64
	OrderId       sql.NullString
	Picked        sql.NullString
	ShipDate      sql.NullTime
	Supplier      sql.NullString
}

type ListSalesHistory struct {
	OrgId      string
	ItemId     string
	LocationId string
}
type QueueSalesHistory struct {
	OrgId           string
	ItemId          string
	LocationId      string
	PostalCode      sql.NullString
	StartDate       time.Time
	SaleQuantity    float32
	Promotion       sql.NullString
	AbnormalDemand  sql.NullString
	AdjustedSaleQty sql.NullFloat64
	TriggerTime     time.Time
}

type MessageSalesHistory struct {
	OrgId           string
	ItemId          string
	LocationId      string
	PostalCode      sql.NullString
	StartDate       time.Time
	SaleQuantity    float32
	Promotion       sql.NullString
	AbnormalDemand  sql.NullString
	AdjustedSaleQty sql.NullFloat64
	InvalidCd       string
}

func main() {

	fmt.Println("start of main")

	token = mustGetenv("PUBSUB_VERIFICATION_TOKEN") // token is used to verify push requests.

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, mustGetenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		fmt.Println("client err: ", err)
		log.Fatal(err)
	}
	defer client.Close()

	http.HandleFunc("/pubsub/push", pushHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	//TODO remove testing
	//fmt.Println("port:", port)

	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func pushHandler(w http.ResponseWriter, r *http.Request) {

	ctx = context.Background()
	var err error
	verified := verifyMessage(w, r)
	if verified {

		// Client
		//dataClient, err = spanner.NewClient(ctx, "projects/rp-database-s-01/instances/rp-combined/databases/retail")
		// Emulator client
		dataClient, err = spanner.NewClient(ctx, "projects/rp-queue-throttle-s-01/instances/rp-combined/databases/retail")
		if err != nil {
			log.Fatal(err)
		}
		defer dataClient.Close()

		fmt.Println("after dataclient")

		messagesMu.Lock()
		defer messagesMu.Unlock()

		//Read forecast and supply queues (first in first out) and publish messages
		//May want to make each of these separate app engines, to stay within the time limit for the cron messages.

		//TODO calculate number of rows to pull from each queue

		queueToMessageFcstSuply("forecast", 100, 10)

		queueToMessageFcstSuply("supply", 175, 10)

		/*
			On-hand/on-order and sales history work differently from the other queues.
			All the rows for a SKU need to remain together in the same message
			This is done using a two-step query and then not breaking into chunks.
			In order to have significant throughput, make multiple calls.
		*/
		var iter int = 30
		for i := 1; i < iter; i++ {
			noQueueRows := queueToMessageSalesHistory()
			if noQueueRows {
				break
			}
		}

		//See explanation above sales history
		iter = 30
		for i := 1; i < iter; i++ {
			noQueueRows := queueToMessageOnHandOnOrder()
			if noQueueRows {
				break
			}
		}

	} else {
		fmt.Println("Invalid token")
	}
}

// Tried using interface to combine these 3 functions into a single function, didn't work
func containsSkuFcstSuply(messages []MessageFcstSuply, new MessageFcstSuply) bool {
	for _, existing := range messages {
		if existing.OrgId == new.OrgId &&
			existing.ItemId == new.ItemId &&
			existing.LocationId == new.LocationId {
			return true
		}
	}
	return false
}

//	func containsSkuOnHandOnOrder(messages []MessageOnHandOnOrder, new MessageOnHandOnOrder) bool {
//		for _, existing := range messages {
//			if existing.OrgId == new.OrgId &&
//				existing.ItemId == new.ItemId &&
//				existing.LocationId == new.LocationId {
//				return true
//			}
//		}
//		return false
//	}
func containsMsgSalesHistory(messages []MessageSalesHistory, new MessageSalesHistory) bool {

	var postalCodeNew string
	var postalCodeExist string

	if new.PostalCode.Valid {
		postalCodeNew = new.PostalCode.String
	} else {
		postalCodeNew = ""
	}

	for _, existing := range messages {

		if existing.PostalCode.Valid {
			postalCodeExist = existing.PostalCode.String
		} else {
			postalCodeExist = ""
		}

		if existing.OrgId == new.OrgId &&
			existing.ItemId == new.ItemId &&
			existing.LocationId == new.LocationId &&
			postalCodeExist == postalCodeNew {
			return true
		}
	}
	return false
}

func mustGetenv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Panicf("%v environment variable not set.", k)
	}
	return v
}
