package httpServer

import (
	"encoding/json"
	"golang-kafka-producer-consumer/producer/entity"
	"net/http"

	log "github.com/sirupsen/logrus"
)

// Health verify if the api is up and running
func (h *httpServer) Health(res http.ResponseWriter, _ *http.Request) {
	json.NewEncoder(res).Encode(entity.Message{MSG: "status up"})
}

// CommitMessage commit a message to kafka queue
func (h *httpServer) CommitMessage(res http.ResponseWriter, req *http.Request) {
	// extract body from http.Request context
	ctx := req.Context().Value("msg_object")
	msg, ok := ctx.(*entity.Message)
	if !ok {
		log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Error("missing body structure")
		HandleError(res, "Invalid data in request", http.StatusBadRequest)
		return
	}

	msgInBytes, err := json.Marshal(msg.MSG)
	if err != nil {
		log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Error(err.Error())
		HandleError(res, "Invalid data in request", http.StatusBadRequest)
		return
	}

	keyInBytes, err := json.Marshal(msg.Sender)
	if err != nil {
		log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Error(err.Error())
		HandleError(res, "Invalid data in request", http.StatusBadRequest)
		return
	}

	response, err := h.ctrl.CommitMessage(msgInBytes, keyInBytes)
	if err != nil {
		log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Error(err.Error())
		HandleCustomError(res, err)
		return
	}
	log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Info("ok")
	json.NewEncoder(res).Encode(response)
}
