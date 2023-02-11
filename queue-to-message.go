package main

// Send pub/sub messages from list of SKUs
func queueToMessageSku(queueType string, selectSize int, chunkSize int) {

	var chunkCounter int
	var messagesPublish []MessageSku
	var published bool

	//Get first in first out from queue
	queueTriggers := createListSku(queueType, selectSize)

	//For each trigger in the queue
	for _, trigger := range queueTriggers {

		message := MessageSku{trigger.OrgId, trigger.ItemId, trigger.LocationId}
		messagesPublish = append(messagesPublish, message)
		chunkCounter++

		if chunkCounter >= chunkSize {
			published = publishTriggersSku(queueType, messagesPublish)
			if published == false {
				return
			}
			//Delete triggers
			deleteQueueFcstSply(queueType, messagesPublish)

			//reset
			chunkCounter = 0
			messagesPublish = nil
		}
	}
	if len(messagesPublish) > 0 {
		published = publishTriggersSku(queueType, messagesPublish)
		if published == false {
			return
		} else {
			//Delete triggers
			deleteQueueFcstSply(queueType, messagesPublish)
		}
	}
}
