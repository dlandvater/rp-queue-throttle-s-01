package main

import (
	"cloud.google.com/go/spanner"
	"fmt"
	"google.golang.org/api/iterator"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func StringReverse(InputString string) (ResultString string) {
	// iterating and prepending
	for _, c := range InputString {
		ResultString = string(c) + ResultString
	}
	return
}

func createRowId(orgId string, tableName string) string {
	var n int64
	var typ string

	//TODO can increase the number of digits in the time component if needed.

	// Type of table
	switch tableName {
	case "error_log":
		typ = "1"
	case "queue_forecast":
		typ = "2"
	case "queue_supply":
		typ = "3"
	case "queue_loadbuilding":
		typ = "4"
	case "queue_on_hand_on_order":
		typ = "5"
	default:
		typ = "0"
	}
	// Create unique row ID using first 14 digits of nano time reversed + orgId + type of table
	time.Sleep(time.Nanosecond)
	n = time.Now().UnixNano()
	s := strconv.FormatInt(n, 10) // Nano time to string
	r := StringReverse(s)         // Reverse time to prevent database hot spots
	t := r[0:15]                  //Truncate high-end digits
	rowId := t + orgId + typ
	return rowId
}

func insertErrorLog(orgId string, itemId string, locationId string, err error, skip int) {

	_, s, n, _ := runtime.Caller(skip)
	ss := strings.Split(s, "/")
	s = ss[len(ss)-1]
	errSource := fmt.Sprintf("%s%s%d", s, ":", n)
	if len(errSource) > 50 {
		errSource = errSource[0:50]
	}
	errMsg := err.Error()
	if len(errMsg) > 1024 {
		errMsg = errMsg[0:1023]
	}
	sCol := []string{"row_id", "org_id", "create_date", "item_id", "location_id", "source", "message"}
	rowId := createRowId(orgId, "error_log")

	m := []*spanner.Mutation{
		spanner.InsertOrUpdate("error_log", sCol, []interface{}{rowId, orgId, time.Now(), itemId, locationId,
			errSource, errMsg}),
	}
	_, err = dataClient.Apply(ctx, m)
	if err != nil {
		log.Printf("write error log rp-queue-throttle", err)
	}
}

func insertBatchLog(orgId string, bMsg string) {

	if len(bMsg) > 1024 {
		bMsg = bMsg[0:1023]
	}
	rowId := createRowId(orgId, "error_log")
	sCol := []string{"row_id", "org_id", "create_date", "message"}

	m := []*spanner.Mutation{
		spanner.InsertOrUpdate("error_log", sCol, []interface{}{rowId, orgId, time.Now(), bMsg}),
	}
	_, err = dataClient.Apply(ctx, m)
	log.Printf("write error log rp-queue-throttle", err)
}

func createSkuInClause(list []ListSku) string {
	var INclause string
	for _, sku := range list {
		if INclause == "" {
			INclause = sku.OrgId + "/" + sku.ItemId + "/" + sku.LocationId
		} else {
			INclause = INclause + "','" + sku.OrgId + "/" + sku.ItemId + "/" + sku.LocationId
		}
	}
	INclause = "('" + INclause + "') "
	return INclause
}

// Get queue triggers for SKU, used for forecast and supply - first in first out
func createListSku(queueType string, selectSize int) []ListSku {

	var list []ListSku

	sSQL := "SELECT org_id, item_id, location_id, MIN(trigger_time) "
	switch queueType {
	case "forecast":
		sSQL = sSQL + "FROM queue_forecast "
	case "supply":
		sSQL = sSQL + "FROM queue_supply "
	case "on_hand_on_order":
		sSQL = sSQL + "FROM queue_on_hand_on_order "
	default:
		fmt.Println("Invalid queue type")
	}
	sSQL = sSQL + fmt.Sprintf("GROUP BY org_id, item_id, location_id "+
		"ORDER BY MIN(trigger_time) DESC LIMIT %v ;", selectSize)

	stmt := spanner.Statement{SQL: sSQL}
	iter := dataClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return list
		}
		if err != nil {
			// No organization has been determined yet
			insertErrorLog("0", "", "", err, 1)
			return list
		}
		nextListRow := ListSku{}
		if err2 := row.Columns(&nextListRow.OrgId, &nextListRow.ItemId, &nextListRow.LocationId,
			&nextListRow.TriggerTime); err2 != nil {
			// No organization has been determined yet
			insertErrorLog("0", "", "", err, 1)
			return list
		}
		list = append(list, nextListRow)
	}
	return list
}

// Delete published triggers from the forecast or supply queue table
func deleteQueueFcstSply(queueType string, messages []MessageSku) {

	var deleteFromTable string

	switch queueType {
	case "forecast":
		deleteFromTable = "queue_forecast"
	case "supply":
		deleteFromTable = "queue_supply"
	default:
		log.Println("Invalid queue type")
		message := messages[0]
		insertErrorLog(message.OrgId, message.ItemId, message.LocationId, err, 1)
	}

	// uses slice of mutations
	var m *spanner.Mutation
	var mm []*spanner.Mutation
	var counter int

	for _, message := range messages {
		m = spanner.Delete(deleteFromTable, spanner.Key{message.OrgId, message.ItemId, message.LocationId})
		mm = append(mm, m)

		// Prevent exceeding mutation max: one mutation per net change trigger
		counter++
		if counter > 5000 {
			_, err := dataClient.Apply(ctx, mm)
			if err != nil {
				insertErrorLog(message.OrgId, message.ItemId, message.LocationId, err, 1)
			}
			fmt.Println("break delete SKU mutation, counter:", counter)
			mm = nil
			counter = 0
		}
	}
	if len(mm) > 0 {
		_, err := dataClient.Apply(ctx, mm)
		if err != nil {
			message := messages[0]
			insertErrorLog(message.OrgId, message.ItemId, message.LocationId, err, 1)
		}
	}
}
