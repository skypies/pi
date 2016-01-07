package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	
	"golang.org/x/net/context"
	"google.golang.org/appengine"

	"github.com/skypies/date"
	"github.com/skypies/geo/sfo"
	"github.com/skypies/pi/airspace"

	"github.com/skypies/flightdb2/ref"
)

func init() {
	http.HandleFunc("/text", nowHandler)
	http.HandleFunc("/json", nowJsonHandler)
	http.HandleFunc("/", nowMapHandler)
}

var(
	kMaxStaleDuration = time.Second * 30
)

// We tart it up with airframe data, and trim out stale entries
func getAirspaceForDisplay(c context.Context) (airspace.Airspace, error) {
	a := airspace.Airspace{}
	if err := a.FromMemcache(c); err != nil {
		return a,err
	}

	airframes := ref.NewAirframeCache(c)
	for k,aircraft := range a.Aircraft {
		age := time.Since(a.Aircraft[k].Msg.GeneratedTimestampUTC)
		if age > kMaxStaleDuration {
			delete(a.Aircraft, k)
		} else if af := airframes.Get(string(k)); af != nil {
			aircraft.Registration = af.Registration
			aircraft.EquipmentType = af.EquipmentType
			a.Aircraft[k] = aircraft
		}
	}

	return a,nil
}

func nowHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	a,err := getAirspaceForDisplay(c)
	if err != nil {
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

func nowJsonHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	a,err := getAirspaceForDisplay(c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data,err := json.Marshal(a)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func nowMapHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	a,err := getAirspaceForDisplay(c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	var params = map[string]interface{}{
		"Legend": buildLegend(),
		"AircraftJS": a.ToJSVar(),
		"MapsAPIKey": "",
		"Center": sfo.KFixes["EPICK"],
		"Zoom": 8,
	}

	if err := templates.ExecuteTemplate(w, "airspace-map", params); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
