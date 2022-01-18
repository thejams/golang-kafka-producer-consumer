package httpServer

import (
	"encoding/json"
	"fmt"
	"golang-kafka-producer-consumer/producer/entity"
	"golang-kafka-producer-consumer/producer/util"
	"io/ioutil"
	"net/http"

	"github.com/go-playground/validator"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func (h *httpServer) initRouter(r *mux.Router) {
	r.HandleFunc("/health", h.Health).Methods("GET")
	r.HandleFunc("/msg", h.CommitMessage).Methods("POST")
}

// Health verify if the api is up and running
func (h *httpServer) Health(res http.ResponseWriter, _ *http.Request) {
	json.NewEncoder(res).Encode(entity.Message{MSG: "status up"})
}

// CommitMessage commit a message to kafka queue
func (h *httpServer) CommitMessage(res http.ResponseWriter, req *http.Request) {
	var msg entity.Message
	reqBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Error(err.Error())
		HandleError(res, "Invalid data in request", http.StatusBadRequest)
		return
	}

	err = json.Unmarshal(reqBody, &msg)
	if err != nil {
		log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Error(err.Error())
		HandleError(res, "Invalid data in request", http.StatusBadRequest)
		return
	}

	err = ValidateFields(msg)
	if err != nil {
		log.WithFields(log.Fields{"package": "httpServer", "method": "CommitMessage"}).Error(err.Error())
		HandleCustomError(res, err)
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

// HandleError handle the errors to be returned to the user
func HandleError(res http.ResponseWriter, err string, httpCode int) {
	res.WriteHeader(httpCode)
	json.NewEncoder(res).Encode(err)
}

// HandleError handle the custom errors to be returned to the user
func HandleCustomError(res http.ResponseWriter, customErr error) {
	status, err := util.DecodeError(customErr)
	res.WriteHeader(status)
	json.NewEncoder(res).Encode(err)
}

//ValidateFields Validate the request object fields
func ValidateFields(req interface{}) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		return &util.BadRequestError{Message: fmt.Sprintf("Los siguientes campos son requeridos: %v", err.Error())}
	}
	return nil
}
