package main

import (
	"html/template"
	"net/http"
	
	"github.com/skypies/pi/airspace/realtime"
)

var templates *template.Template

func init() {
	templates = LoadTemplates("templates")
	http.HandleFunc("/", handleWithTemplates(realtime.AirspaceHandler))
}

// This stuff should probbaly be inside skypies/util
type baseHandler     func(http.ResponseWriter, *http.Request)
type templateHandler func(http.ResponseWriter, *http.Request, *template.Template)
func handleWithTemplates(th templateHandler) baseHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		th(w,r,templates)
	}
}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
