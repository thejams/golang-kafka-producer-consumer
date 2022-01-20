package httpServer

import (
	"encoding/json"
	"fmt"
	"golang-kafka-producer-consumer/producer/util"
	"net/http"

	"github.com/go-playground/validator"
	"github.com/gorilla/mux"
)

func (h *httpServer) initRouter(r *mux.Router) {
	healthRouter := r.PathPrefix("/api/").Subrouter()
	healthRouter.HandleFunc("/health", h.Health).Methods("GET")

	msgRouter := r.PathPrefix("/kafka/").Subrouter()
	msgRouter.HandleFunc("/msg", h.CommitMessage).Methods("POST")
	msgRouter.Use(validateMsgFieldsMiddleware)
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
