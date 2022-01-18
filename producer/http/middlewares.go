package httpServer

import (
	"context"
	"encoding/json"
	"golang-kafka-producer-consumer/producer/entity"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"
)

//validateMsgFieldsMiddleware Validate the request object fields
func validateMsgFieldsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		msg := new(entity.Message)
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

		reqCtx := context.WithValue(req.Context(), "msg_object", msg)
		next.ServeHTTP(res, req.WithContext(reqCtx))

	})
}
