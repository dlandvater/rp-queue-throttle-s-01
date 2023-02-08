package main

import (
	"cloud.google.com/go/spanner"
	"database/sql"
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
		spanner.InsertOrUpdate("error_log", sCol, []interface{}{rowId, orgId, itemId, locationId, time.Now(),
			errSource, errMsg}),
	}
	_, err = dataClient.Apply(ctx, m)
	log.Printf("write error log rp-queue-throttle", err)
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

// Get queue triggers forecast or supply - first in first out
func queryQueueRowsFcstSuply(queueType string, selectSize int) ([]QueueFcstSuply, int32) {

	var queue []QueueFcstSuply
	var count int32 = 0

	sSQL := "SELECT org_id, item_id, location_id, MIN(trigger_time) "
	switch queueType {
	case "forecast":
		sSQL = sSQL + "FROM queue_forecast "
	case "supply":
		sSQL = sSQL + "FROM queue_supply "
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
			return queue, count
		}
		if err != nil {
			// No organization has been determined yet
			insertErrorLog("0", "", "", err, 1)
			return queue, count
		}
		nextQueueRow := QueueFcstSuply{}
		if err2 := row.Columns(&nextQueueRow.OrgId, &nextQueueRow.ItemId, &nextQueueRow.LocationId,
			&nextQueueRow.TriggerTime); err2 != nil {
			// No organization has been determined yet
			insertErrorLog("0", "", "", err, 1)
			return queue, count
		}
	}
	return queue, count
}

// Get queue triggers for on-hand on-order - first in first out
func queryQueueRowsOnHandOnOrder(selectSize int) ([]QueueOnHandOnOrder, []ListOnHandOnOrder) {

	//TODO may need to change this to a series of single SKU queries if the IN clause list gets too large (selectSize)

	//See explanation in calling function, works differently from the forecast/supply queues.

	//Step 1 - get list of SKUs
	list := createOnHandOnOrderList(selectSize)

	//Step 2 - Create IN clause from list of SKUs
	INclause := createInClause(list)

	//Part 3 - select the rows from the list
	queue := createOnHandOnOrderQueue(INclause, list)
	return queue, list
}

func createOnHandOnOrderList(selectSize int) []ListOnHandOnOrder {

	var list []ListOnHandOnOrder

	//Should only be one OH row per SKU
	sSQL := fmt.Sprintf("SELECT org_id, item_id, location_id "+
		"FROM queue_on_hand_on_order "+
		"WHERE type = 'OH' "+
		"ORDER BY trigger_time DESC LIMIT %v ;", selectSize)

	stmt := spanner.Statement{SQL: sSQL}
	iter := dataClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return list
		}
		if err != nil {
			// No organization has been selected yet
			insertErrorLog("0", "", "", err, 1)
			return list
		}
		nextListRow := ListOnHandOnOrder{}
		err2 := row.Columns(&nextListRow.OrgId, &nextListRow.ItemId, &nextListRow.LocationId)
		if err2 != nil {
			// No organization has been determined yet
			insertErrorLog("0", "", "", err, 1)
			return list
		}
		list = append(list, nextListRow)
	}
	return list
}

func createInClause(list []ListOnHandOnOrder) string {
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

func createOnHandOnOrderQueue(INclause string, list []ListOnHandOnOrder) []QueueOnHandOnOrder {

	var queue []QueueOnHandOnOrder
	var count int

	sSQL := "SELECT org_id, type, item_id, location_id, on_hand, due_date, quantity, " +
		"order_id, picked, ship_date, supplier_id, trigger_time " +
		"FROM queue_on_hand_on_order " +
		"WHERE CONCAT(org_id, '/', item_id, '/', location_id) IN " + INclause +
		"ORDER BY org_id, item_id, location_id, type ;"

	stmt := spanner.Statement{SQL: sSQL}
	iter := dataClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return queue
		}
		if err != nil {
			// No organization has been determined yet
			insertErrorLog("0", "", "", err, 1)
			return queue
		}
		nextQueueRow := QueueOnHandOnOrder{}
		if err2 := row.Columns(&nextQueueRow.OrgId, &nextQueueRow.TypeId, &nextQueueRow.ItemId, &nextQueueRow.LocationId,
			&nextQueueRow.OnHand, &nextQueueRow.DueDate, &nextQueueRow.OrderQuantity, &nextQueueRow.OrderId,
			&nextQueueRow.Picked, &nextQueueRow.ShipDate, &nextQueueRow.Supplier, &nextQueueRow.TriggerTime); err2 != nil {
			// No organization has been selected yet
			insertErrorLog("0", "", "", err, 1)
			return queue
		}
		queue = append(queue, nextQueueRow)
		count++
	}
	return queue
}

// Get queue triggers for sales history - first in first out
func queryQueueRowsSalesHistory(selectSize int) ([]QueueSalesHistory, []ListSalesHistory, int32) {

	var list []ListSalesHistory
	var INclause string
	var queue []QueueSalesHistory
	var count int32 = 0

	//TODO may need to change this to a series of single SKU queries if the IN clause list gets too large (selectSize)

	//Should only be one weekly sales history row per item/loction/postal code, additional rows will be rejected in monitor-sh
	//See explanation in calling function, works differently from the forecast/supply queues.

	//Step 1 - get list of SKUs
	//Note: could be duplicates in this list
	sSQL1 := fmt.Sprintf("SELECT org_id, item_id, location_id "+
		"FROM queue_sales_history "+
		"ORDER BY trigger_time DESC LIMIT %v ;", selectSize)

	rows1, err1 := db.Query(sSQL1)
	defer rows1.Close()
	if err1 != nil {
		sourceString := errorSource(err1, 1)
		fmt.Println("Error preparing query queue sales history 1: ", "", "", "", sourceString, err1)
		errorLog := ErrorLog{"", "", "", 1, err1}
		insertErrorLog(errorLog, db)
		return queue, list, count
	}

	for rows1.Next() {
		nextListRow := ListSalesHistory{}
		err2 := rows1.Scan(&nextListRow.OrgId, &nextListRow.ItemId, &nextListRow.LocationId)
		if err2 != nil {
			sourceString := errorSource(err2, 1)
			fmt.Println("Error query queue sales history 2: ", "", "", "", sourceString, err2)
			errorLog := ErrorLog{"", "", "", 1, err2}
			insertErrorLog(errorLog, db)
			return queue, list, count
		}
		list = append(list, nextListRow)
	}

	//Step 2 - Create IN clause from list of SKUs
	for _, sku := range list {
		if INclause == "" {
			INclause = sku.OrgId + "/" + sku.ItemId + "/" + sku.LocationId
		} else {
			INclause = INclause + "','" + sku.OrgId + "/" + sku.ItemId + "/" + sku.LocationId
		}
	}
	INclause = "('" + INclause + "') "

	//Part 3 - select the rows from the list
	sSQL3 := "SELECT org_id, item_id, location_id, postal_code, start_date, sale_qty, " +
		"promotion, abnormal_demand, adjusted_sale_qty, trigger_time " +
		"FROM queue_sales_history " +
		"WHERE CONCAT_WS('/', org_id, item_id, location_id) IN " + INclause +
		"ORDER BY org_id, item_id, location_id, postal_code ;"

	rows3, err3 := db.Query(sSQL3)
	defer rows3.Close()
	if err3 != nil {
		sourceString := errorSource(err3, 1)
		fmt.Println("Error preparing query queue sales history 3: ", "", "", "", sourceString, err3)
		errorLog := ErrorLog{"", "", "", 1, err3}
		insertErrorLog(errorLog, db)
		return queue, list, count
	}

	for rows3.Next() {
		nextQueueRow := QueueSalesHistory{}
		err4 := rows3.Scan(&nextQueueRow.OrgId, &nextQueueRow.ItemId, &nextQueueRow.LocationId,
			&nextQueueRow.PostalCode, &nextQueueRow.StartDate, &nextQueueRow.SaleQuantity, &nextQueueRow.Promotion,
			&nextQueueRow.AbnormalDemand, &nextQueueRow.AdjustedSaleQty, &nextQueueRow.TriggerTime)

		if err4 != nil {
			sourceString := errorSource(err4, 1)
			fmt.Println("Error query queue sales history 4: ", "", "", "", sourceString, err4)
			errorLog := ErrorLog{"", "", "", 1, err4}
			insertErrorLog(errorLog, db)
			return queue, list, count
		}
		queue = append(queue, nextQueueRow)
		count++
	}
	return queue, list, count
}

// Delete published forecast / supply triggers from the queue
func deleteQueueRowsFcstSuply(queueType string, message MessageFcstSuply) int32 {

	var sSQL string
	var err error
	var res sql.Result

	if QueueDeleteStmtFcst == nil {
		sSQL = "DELETE FROM queue_forecast WHERE org_id = ? AND item_id = ? AND location_id = ? ;"
		QueueDeleteStmtFcst, err = db.Prepare(sSQL)
		if err != nil {
			sourceString := errorSource(err, 1)
			fmt.Println("Error preparing delete queue fcst:", message.OrgId, message.ItemId, message.LocationId, sourceString, err)
			errorLog := ErrorLog{message.OrgId, message.ItemId, message.LocationId, 1, err}
			insertErrorLog(errorLog, db)
			return 0
		}
	}
	if QueueDeleteStmtSuply == nil {
		sSQL = "DELETE FROM queue_supply WHERE org_id = ? AND item_id = ? AND location_id = ? ;"
		QueueDeleteStmtSuply, err = db.Prepare(sSQL)
		if err != nil {
			sourceString := errorSource(err, 1)
			fmt.Println("Error preparing delete queue suply:", message.OrgId, message.ItemId, message.LocationId, sourceString, err)
			errorLog := ErrorLog{message.OrgId, message.ItemId, message.LocationId, 1, err}
			insertErrorLog(errorLog, db)
			return 0
		}
	}

	switch queueType {
	case "forecast":
		res, err = QueueDeleteStmtFcst.Exec(message.OrgId, message.ItemId, message.LocationId)
	case "supply":
		res, err = QueueDeleteStmtSuply.Exec(message.OrgId, message.ItemId, message.LocationId)
	default:
		fmt.Println("Invalid queue type")
	}
	if err != nil {
		sourceString := errorSource(err, 1)
		fmt.Println("Error delete queue:", message.OrgId, message.ItemId, message.LocationId, sourceString, err)
		errorLog := ErrorLog{message.OrgId, message.ItemId,
			message.LocationId, 1, err}
		insertErrorLog(errorLog, db)
		return 0
	}

	// affected rows
	rowCount, err := res.RowsAffected()
	if err != nil {
		return 0
	}

	return int32(rowCount)
}

// Delete published on-hand on-order triggers from the queue
func deleteQueueRowsOnHandOnOrder(sku ListOnHandOnOrder) int32 {

	var err error

	sSQL := "DELETE FROM queue_on_hand_on_order WHERE org_id = ? AND item_id = ? AND location_id = ? ;"

	if QueueDeleteStmtOnHandOnOrder == nil {
		QueueDeleteStmtOnHandOnOrder, err = db.Prepare(sSQL)
		if err != nil {
			sourceString := errorSource(err, 1)
			fmt.Println("Error preparing delete queue OH OO:", sku.OrgId, sku.ItemId, sku.LocationId, sourceString, err)
			errorLog := ErrorLog{sku.OrgId, sku.ItemId, sku.LocationId, 1, err}
			insertErrorLog(errorLog, db)
			return 0
		}
	}

	res, err := QueueDeleteStmtOnHandOnOrder.Exec(sku.OrgId, sku.ItemId, sku.LocationId)
	if err != nil {
		sourceString := errorSource(err, 1)
		fmt.Println("Error delete queue OH OO:", sku.OrgId, sku.ItemId, sku.LocationId, sourceString, err)
		errorLog := ErrorLog{sku.OrgId, sku.ItemId, sku.LocationId, 1, err}
		insertErrorLog(errorLog, db)
		return 0
	}

	// affected rows
	rowCount, err := res.RowsAffected()
	if err != nil {
		return 0
	}

	return int32(rowCount)
}

// Delete published sales history triggers from the queue
func deleteQueueRowsSalesHistory(sku ListSalesHistory) int32 {

	var err error

	sSQL := "DELETE FROM queue_sales_history WHERE org_id = ? AND item_id = ? AND location_id = ? ;"

	if QueueDeleteStmtSalesHistory == nil {
		QueueDeleteStmtSalesHistory, err = db.Prepare(sSQL)
		if err != nil {
			sourceString := errorSource(err, 1)
			fmt.Println("Error preparing delete queue SH:", sku.OrgId, sku.ItemId, sku.LocationId, sourceString, err)
			errorLog := ErrorLog{sku.OrgId, sku.ItemId, sku.LocationId, 1, err}
			insertErrorLog(errorLog, db)
			return 0
		}
	}

	res, err := QueueDeleteStmtSalesHistory.Exec(sku.OrgId, sku.ItemId, sku.LocationId)
	if err != nil {
		sourceString := errorSource(err, 1)
		fmt.Println("Error delete queue SH:", sku.OrgId, sku.ItemId, sku.LocationId, sourceString, err)
		errorLog := ErrorLog{sku.OrgId, sku.ItemId, sku.LocationId, 1, err}
		insertErrorLog(errorLog, db)
		return 0
	}

	// affected rows
	rowCount, err := res.RowsAffected()
	if err != nil {
		return 0
	}

	return int32(rowCount)
}
