package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

// Publishes SKU messages
func publishTriggersSku(queueType string, messages []MessageSku) bool {

	var topicName string

	switch queueType {
	case "forecast":
		topicName = os.Getenv("PUBSUB_TOPIC_FORECAST")
	case "supply":
		topicName = os.Getenv("PUBSUB_TOPIC_SUPPLY")
	case "on_hand_on_order":
		topicName = os.Getenv("PUBSUB_TOPIC_ON_HAND_ON_ORDER")
	case "sales_history":
		topicName = os.Getenv("PUBSUB_TOPIC_SALES_HISTORY")
	default:
		log.Println("Invalid queue type")
		msg := messages[0]
		insertErrorLog(msg.OrgId, msg.ItemId, msg.LocationId, err, 1)
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		log.Printf("NewClient error", err)
		msg := messages[0]
		insertErrorLog(msg.OrgId, msg.ItemId, msg.LocationId, err, 1)
		return false
	}
	defer client.Close()
	topic = client.Topic(topicName)

	var j []byte
	j, err = json.Marshal(messages)

	msg := &pubsub.Message{
		Data: []byte(j),
		ID:   "",
	}

	var result *pubsub.PublishResult
	result = topic.Publish(ctx, msg)
	ID, err := result.Get(ctx)
	if err != nil {
		log.Printf("topic.Publish error", err)
		msg := messages[0]
		insertErrorLog(msg.OrgId, msg.ItemId, msg.LocationId, err, 1)
		return false
	}
	//TODO remove testing
	fmt.Println("triggers:", messages)
	fmt.Println("published message, topic, ID", topicName, ID)
	return true
}
