package status

import (
	"log"
	"net/http"
)

func Health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	_, err := w.Write([]byte("{\"status\":\"UP\"}"))
	if err != nil {
		log.Printf("Error writing status response: %v", err)
	}
}
