package httpServer

import (
	"golang-kafka-producer-consumer/producer/controller"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

type httpServer struct {
	ctrl        controller.Controller
	Credentials handlers.CORSOption
	Methods     handlers.CORSOption
	Origins     handlers.CORSOption
	Router      *mux.Router
}

//NewHTTPServer initialice a new http server
func NewHTTPServer(ctrl controller.Controller) *httpServer {
	log.SetFormatter(&log.JSONFormatter{})
	http_server := new(httpServer)

	{
		router := mux.NewRouter().StrictSlash(true)
		router.Use(setHeadersMiddleware)
		http_server.initRouter(router)

		credentials := handlers.AllowCredentials()
		methods := handlers.AllowedMethods([]string{"POST", "GET", "PUT", "DELETE"})
		origins := handlers.AllowedMethods([]string{"*"})

		http_server.Credentials = credentials
		http_server.Methods = methods
		http_server.Origins = origins
		http_server.Router = router
		http_server.ctrl = ctrl
	}

	return http_server
}

func setHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		next.ServeHTTP(w, r)
	})
}
