package util

import (
	"fmt"
	"net/http"
)

//BadRequestError
type BadRequestError struct {
	Message string
}

//NotFoundError
type NotFoundError struct {
	Message string
}

func (e *BadRequestError) Error() string {
	return fmt.Sprintf("BadRequest: %v", e.Message)
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("NotFound: %v", e.Message)
}

//ErrorWrapper main struct for custom error return
type ErrorWrapper struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

//DecodeError returns a specific type of error according to type
func DecodeError(err error) (status int, errBody interface{}) {
	switch err.(type) {
	case *BadRequestError:
		return http.StatusBadRequest, ErrorWrapper{Code: "400", Message: err.Error()}
	case *NotFoundError:
		return http.StatusNotFound, ErrorWrapper{Code: "404", Message: err.Error()}
	default:
		return http.StatusInternalServerError, ErrorWrapper{Code: "500", Message: err.Error()}
	}
}
