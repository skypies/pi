package main

import (
	"encoding/json"
	"net/http"
	"time"
	
	"golang.org/x/net/context"
	"google.golang.org/appengine"

	"github.com/skypies/util/date"
	"github.com/skypies/geo"
	"github.com/skypies/geo/sfo"
	"github.com/skypies/pi/airspace"

	"github.com/skypies/flightdb2/ref"
)

func init() {
	http.HandleFunc("/", rootHandler)
}

var(
	kMaxStaleDuration = time.Second * 30
	kMaxStaleScheduleDuration = time.Minute * 20
)

// We tart it up with airframe and schedule data, trim out stale entries, and trim to fit box
func getAirspaceForDisplay(c context.Context, bbox geo.LatlongBox) (airspace.Airspace, error) {
	a := airspace.Airspace{}
	if err := a.JustAircraftFromMemcache(c); err != nil {
		return a,err
	}

	airframes := ref.NewAirframeCache(c)
	schedules := ref.NewScheduleCache(c)
	for k,aircraft := range a.Aircraft {
		age := time.Since(a.Aircraft[k].Msg.GeneratedTimestampUTC)
		if age > kMaxStaleDuration {
			delete(a.Aircraft, k)
			continue
		}
		if !bbox.SW.IsNil() && !bbox.Contains(aircraft.Msg.Position) {
			delete(a.Aircraft, k)
			continue
		}
		if af := airframes.Get(string(k)); af != nil {
			// Update entry in map to include the airframe data we just found
			aircraft.Airframe = *af
			a.Aircraft[k] = aircraft
		}

		if schedules != nil && time.Since(schedules.LastUpdated) < kMaxStaleScheduleDuration {
			if fs := schedules.Get(string(k)); fs != nil {
				aircraft.Schedule = fs.Identity.Schedule
				a.Aircraft[k] = aircraft
			}
		}
	}

	return a,nil
}

func buildLegend() string {
	legend := date.NowInPdt().Format("15:04:05 MST (2006/01/02)")
	return legend
}

// /?json=1&box_sw_lat=36.1&box_sw_long=-122.2&box_ne_lat=37.1&box_ne_long=-121.5
func rootHandler(w http.ResponseWriter, r *http.Request) {	
	if r.FormValue("json") != "" {
		jsonOutputHandler(w,r)
		return
	}
//	} else if r.FormValue("text") != "" {
//		w.Header().Set("Content-Type", "text/plain")
//		w.Write([]byte(fmt.Sprintf("OK\n * Airspace\n%s\n", a)))
//		return

	var params = map[string]interface{}{
		"MapsAPIKey": "",
		"Center": sfo.KFixes["YADUT"],
		"Zoom": 9,
		"URLToPoll": "http://fdb.serfr1.org/?json=1",
	}

	if err := templates.ExecuteTemplate(w, "airspace-map-poller", params); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
/*
	var params = map[string]interface{}{
		"Legend": buildLegend(),
		"AircraftJS": a.ToJSVar(r.URL.Host, time.Now().Add(-30 * time.Second)),
		"MapsAPIKey": "",
		"Center": sfo.KFixes["YADUT"],
		"Zoom": 9,
	}

	if err := templates.ExecuteTemplate(w, "airspace-map", params); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
*/
}

func jsonOutputHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)	
	a,err := getAirspaceForDisplay(c, geo.FormValueLatlongBox(r, "box"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Temporary hack, to let goapp serve'd things call the deployed version of this URL
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers",
		"Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	data,err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
	return
}
