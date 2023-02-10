package main

import (
	"cloud.google.com/go/pubsub"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

var (
	topic *pubsub.Topic

	// Messages received by this instance.
	messagesMu sync.Mutex
	messages   []string
)

type pushRequest struct {
	Message struct {
		Attributes map[string]string
		Data       []byte
		ID         string `json:"message_id"`
	}
	Subscription string
}

// Handles incoming message to check for SKUs that need to be re-planned
func verifyMessage(w http.ResponseWriter, r *http.Request) bool {

	// Verify the token.
	if r.URL.Query().Get("token") != token {
		http.Error(w, "Bad token", http.StatusBadRequest)
		return false
	}
	msg := &pushRequest{}
	if err := json.NewDecoder(r.Body).Decode(msg); err != nil {
		http.Error(w, fmt.Sprintf("Could not decode body: %v", err), http.StatusBadRequest)
		return true
	}
	MsgId = msg.Message.ID
	log.Printf("message ID", MsgId)
	return true
}
