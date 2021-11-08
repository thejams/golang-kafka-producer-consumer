package httpServer

import (
	"encoding/json"
	"golang-kafka-producer-consumer/src/entity"
	"net/http"

	log "github.com/sirupsen/logrus"
)

type HTTPServer interface {
	Health(res http.ResponseWriter, _ *http.Request)
}

type httpServer struct{}

//NewHTTPServer initialice a new http server
func NewHTTPServer() HTTPServer {
	log.SetFormatter(&log.JSONFormatter{})
	return &httpServer{}
}

// Health verify if the api is up and running
func (h *httpServer) Health(res http.ResponseWriter, _ *http.Request) {
	json.NewEncoder(res).Encode(entity.Message{MSG: "status up"})
}
