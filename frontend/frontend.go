package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
	
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/urlfetch"

	"github.com/skypies/adsb"
	"github.com/skypies/geo"
	"github.com/skypies/geo/sfo"
	"github.com/skypies/pi/airspace"

	"github.com/skypies/flightdb2/ref"
	//"github.com/skypies/flightdb2/fa"
	"github.com/skypies/flightdb2/fr24"
)

func init() {
	http.HandleFunc("/", rootHandler)
}

var(
	kMaxStaleDuration = time.Second * 30
	kMaxStaleScheduleDuration = time.Minute * 20
)

// {{{ rootHandler

// /?json=1&box_sw_lat=36.1&box_sw_long=-122.2&box_ne_lat=37.1&box_ne_long=-121.5
func rootHandler(w http.ResponseWriter, r *http.Request) {	
	if r.FormValue("json") != "" {
		jsonOutputHandler(w,r)
		return
	}

	url := "http://fdb.serfr1.org/?json=1"
	if r.FormValue("comp") != "" {
		url += "&comp=1"
	}
		
	var params = map[string]interface{}{
		"MapsAPIKey": "",
		"Center": sfo.KFixes["YADUT"],
		"Zoom": 9,
		"URLToPoll": url,
	}

	if err := templates.ExecuteTemplate(w, "map-poller", params); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// }}}
// {{{ jsonOutputHandler

func jsonOutputHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)	
	as,err := getAirspaceForDisplay(c, geo.FormValueLatlongBox(r, "box"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if r.FormValue("comp") != "" {
		addFr24ToAirspace(c, &as)

		//if r.FormValue("fa") != "" { faToAirspace(c, &as) }

		// Weed out stale stuff (mostly from fa)
		for k,_ := range as.Aircraft {
			age := time.Since(as.Aircraft[k].Msg.GeneratedTimestampUTC)
			if age > kMaxStaleDuration * 2 {
				delete(as.Aircraft, k)
			}
		}
	}
	
	// Temporary hack, to let goapp serve'd things call the deployed version of this URL
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers",
		"Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	data,err := json.MarshalIndent(as, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// }}}

// {{{ getAirspaceForDisplay

// We tart it up with airframe and schedule data, trim out stale entries, and trim to fit box
func getAirspaceForDisplay(c context.Context, bbox geo.LatlongBox) (airspace.Airspace, error) {
	a := airspace.NewAirspace()
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

// }}}

// {{{ addFr24ToAirspace

func addFr24ToAirspace(ctx context.Context, as *airspace.Airspace) {
	fr,_ := fr24.NewFr24(urlfetch.Client(ctx))

	if asFr24,err := fr.FetchAirspace(sfo.KAirports["KSFO"].Box(250,250)); err != nil {
		return
	} else {
		for k,ad := range asFr24.Aircraft {
			// FIXME: This whole thing is a crock. Track down usage of fr24/icaoids and rewrite all of it
			newk := string(k)
			newk = "EE" + strings.TrimPrefix(newk, "EE") // Remove (if present), then add
			ad.Airframe.Icao24 = newk
			as.Aircraft[adsb.IcaoId(newk)] = ad
		}
	}
}

// }}}
// {{{ addFAToAirspace

func addFAToAirspace() {
/*	
var(
	TestAPIKey = "foo"
	TestAPIUsername = "bar"
)

// Overlays the flightaware 'Search' results into the airspace
func faToAirspace(c context.Context, as *airspace.Airspace) string {
	str := ""
	
	myFa := fa.Flightaware{APIKey:TestAPIKey, APIUsername:TestAPIUsername, Client:urlfetch.Client(c)}
	myFa.Init()
}
*/
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
