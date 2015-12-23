package main

import (
	"errors"
	"html/template"
	"fmt"
	"net/http"
	"time"
	
	"google.golang.org/appengine"

	"github.com/skypies/date"
	"github.com/skypies/geo/sfo"
	"github.com/skypies/pi/airspace"
)

var (
	kGoogleMapsAPIKey = "AIzaSyBCNj05xH-7CAdVEXXSPpt2lGDmaynIOBU"

	templates = template.Must(template.New("").Funcs(template.FuncMap{
		"dict": templateDict,
		"formatPdt": templateFormatPdt,
	}).ParseGlob("templates/*"))
)
func templateDict(values ...interface{}) (map[string]interface{}, error) {
	if len(values)%2 != 0 { return nil, errors.New("invalid dict call")	}
	dict := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i+=2 {
		key, ok := values[i].(string)
		if !ok { return nil, errors.New("dict keys must be strings") }
		dict[key] = values[i+1]
	}
	return dict, nil
}
func templateFormatPdt(t time.Time, format string) string {
	return date.InPdt(t).Format(format)
}

func init() {
	http.HandleFunc("/text", nowHandler)
	http.HandleFunc("/", nowMapHandler)
}

func nowHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	a := airspace.Airspace{}
	if err := a.FromMemcache(c); err != nil {
		w.Write([]byte(fmt.Sprintf("not OK: fetch fail: %v\n", err)))
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(fmt.Sprintf("OK\n * Airspace\n%s\n", a)))
}

func buildLegend() string {
	legend := date.NowInPdt().Format("15:04:05 MST (2006/01/02)")
	return legend
}

func nowMapHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	a := airspace.Airspace{}
	if err := a.FromMemcache(c); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var params = map[string]interface{}{
		"Legend": buildLegend(),
		"FlightsJS": a.ToJSVar(),
		"MapsAPIKey": "",//kGoogleMapsAPIKey,
		"Center": sfo.KFixes["EPICK"], //sfo.KLatlongSFO,
		"Zoom": 8,
	}

	templateName := "airspace-map"		
	if err := templates.ExecuteTemplate(w, templateName, params); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
