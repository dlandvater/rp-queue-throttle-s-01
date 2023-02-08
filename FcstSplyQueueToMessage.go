package main

import (
	"fmt"
)

func queueToMessageFcstSuply(queueType string, selectSize int, chunkSize int) {

	var chunkCounter int
	var messagesPublish []MessageFcstSuply

	//Get first in first out from queue
	queueTrigger, _ := queryQueueRowsFcstSuply(queueType, selectSize)

	//For each trigger in the queue
	for _, trigger := range queueTrigger {

		message := MessageFcstSuply{trigger.OrgId, trigger.ItemId,
			trigger.LocationId}
		exists := containsSkuFcstSuply(messagesPublish, message)
		if exists == false {
			messagesPublish = append(messagesPublish, message)
		}
		chunkCounter++

		if chunkCounter >= chunkSize {
			err := publishTriggersFcstSuply(queueType, messagesPublish)
			if err != nil {
				fmt.Println("error in triggers:", err)
			}
			//Delete triggers
			for _, trigger := range messagesPublish {
				_ = deleteQueueRowsFcstSuply(queueType, trigger)
			}
			//reset
			chunkCounter = 0
			messagesPublish = nil
		}
	}

	if len(messagesPublish) > 0 {
		err := publishTriggersFcstSuply(queueType, messagesPublish)
		if err != nil {
			fmt.Println("error in publish triggers:", err)
		}
		//insertQueueFcstSuply(queueType, messagesPublish)
	}
	//Delete triggers
	for _, trigger := range messagesPublish {
		_ = deleteQueueRowsFcstSuply(queueType, trigger)
	}
}

func queueToMessageOnHandOnOrder() bool {
	var queue []QueueOnHandOnOrder
	var list []ListOnHandOnOrder
	var messagesPublish []MessageOnHandOnOrder

	var selectSize int = 20

	//See explanation in calling function, works differently from the forecast/supply queues.

	//Get first in first out from queue
	queue, list = queryQueueRowsOnHandOnOrder(selectSize)

	if list == nil {
		return true
	}

	//For each trigger in the queue
	for _, trigger := range queue {

		message := MessageOnHandOnOrder{trigger.TypeId, trigger.OrgId, trigger.ItemId,
			trigger.LocationId, trigger.OnHand, trigger.DueDate,
			trigger.OrderQuantity, trigger.OrderId, trigger.Picked, trigger.ShipDate,
			trigger.Supplier}
		//No checking for whether the SKU already exists in the message.
		//Multiple rows for each SKU can exist (one on-hand row, and zero to many on-order rows)
		messagesPublish = append(messagesPublish, message)
	}

	if len(messagesPublish) > 0 {
		err := publishTriggersOnHandOnOrder(messagesPublish)
		if err != nil {
			fmt.Println("error in publish OH OO triggers:", err)
		}
		//TODO REMOVE
		fmt.Println("write msg ID, #", MsgId, len(messagesPublish))
	}

	//Delete triggers
	for _, sku := range list {
		_ = deleteQueueRowsOnHandOnOrder(sku)
	}
	return false
}

func queueToMessageSalesHistory() bool {

	var queue []QueueSalesHistory
	var list []ListSalesHistory
	var messagesPublish []MessageSalesHistory
	var selectSize int = 20

	//See explanation in calling function, works differently from the forecast/supply queues.

	//Get first in first out from queue
	queue, list, _ = queryQueueRowsSalesHistory(selectSize)

	if list == nil {
		return true
	}

	//For each trigger in the queue
	for _, trigger := range queue {
		message := MessageSalesHistory{trigger.OrgId, trigger.ItemId,
			trigger.LocationId, trigger.PostalCode, trigger.StartDate,
			trigger.SaleQuantity, trigger.Promotion,
			trigger.AbnormalDemand, trigger.AdjustedSaleQty, ""}
		exists := containsMsgSalesHistory(messagesPublish, message)
		if exists == false {
			messagesPublish = append(messagesPublish, message)
		}
	}

	if len(messagesPublish) > 0 {
		err := publishTriggersSalesHistory(messagesPublish)
		if err != nil {
			fmt.Println("error in publish triggers SH:", err)
		}
		//TODO REMOVE
		fmt.Println("write msg ID, #", MsgId, len(messagesPublish))
	}

	//Delete remaining triggers
	for _, sku := range list {
		_ = deleteQueueRowsSalesHistory(sku)
	}
	return false
}
