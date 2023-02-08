package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"log"
)

// Publishes net change forecast or supply triggers
func publishTriggersFcstSuply(queueType string, messages []MessageFcstSuply) error {

	var err error
	var topicName string

	switch queueType {
	case "forecast":
		topicName = mustGetenv("PUBSUB_TOPIC_FORECAST")
	case "supply":
		topicName = mustGetenv("PUBSUB_TOPIC_SUPPLY")
	default:
		fmt.Println("Invalid queue type")
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, mustGetenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		log.Fatal(err)
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
		//TODO remove testing
		fmt.Println("error topic.Publish:", err)
		return err
	}
	//TODO remove testing
	fmt.Println("triggers:", messages)
	fmt.Println("published message, topic, ID", topicName, ID)

	return err
}

// Publishes on-hand/on-order triggers
func publishTriggersOnHandOnOrder(messages []MessageOnHandOnOrder) error {

	var err error
	var topicName = mustGetenv("PUBSUB_TOPIC_ON_HAND_ON_ORDER")

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, mustGetenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		log.Fatal(err)
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
		//TODO remove testing
		fmt.Println("error topic.Publish OH OO:", err)
		return err
	}
	//TODO remove testing
	fmt.Println("triggers OH OO:", messages)
	fmt.Println("published message, ID:", ID)

	return err
}

// Publishes sales history triggers
func publishTriggersSalesHistory(messages []MessageSalesHistory) error {

	var err error
	var topicName = mustGetenv("PUBSUB_TOPIC_SALES_HISTORY")

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, mustGetenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		log.Fatal(err)
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
		//TODO remove testing
		fmt.Println("error topic.Publish:", err)
		return err
	}
	//TODO remove testing
	fmt.Println("triggers SH:", messages)
	fmt.Println("published message, ID:", ID)

	return err
}
