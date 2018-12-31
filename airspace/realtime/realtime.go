// Package airspace/realtime is glue code for fetching a realtime view of the sky from skypi
// and other online sources, and presenting it on a google map (or as json). It includes
// a http.HandleFunc.
package realtime

import (
	"encoding/json"
	"fmt"
	"html/template"
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

	"github.com/skypies/flightdb/ref"
	"github.com/skypies/flightdb/fr24"
	"github.com/skypies/flightdb/aex"
)

var(
	kMaxStaleDuration = time.Second * 30
	kMaxStaleScheduleDuration = time.Minute * 20
)

// {{{ AirspaceHandler

// AirspaceHandler is a template handler that renders a google maps
// page, which will start polling for realtime flight positions.
// Requires the "map-poller" template and friends. If passed the `json=1` argument,
// will return a JSON rendering of the current state of the sky.
//
// /?json=1&box_sw_lat=36.1&box_sw_long=-122.2&box_ne_lat=37.1&box_ne_long=-121.5
//  &comp=1      (for fr24)
//  &icao=AF1212 (for limiting heatmaps to one aircraft)
func AirspaceHandler(w http.ResponseWriter, r *http.Request, templates *template.Template) {	
	if r.FormValue("json") != "" {
		jsonOutputHandler(w,r)
		return
	}

	url := "http://fdb.serfr1.org/?json=1"
	// Propagate certain URL args to the JSON handler
	for _,key := range []string{"comp","fr24","fa"} {
		if r.FormValue(key) != "" {
			url += fmt.Sprintf("&%s=%s", key, r.FormValue(key))
		}
	}

	var params = map[string]interface{}{
		"MapsAPIKey": "AIzaSyBCNj05xH-7CAdVEXXSPpt2lGDmaynIOBU",
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
	box := geo.FormValueLatlongBox(r, "box")
	src := r.FormValue("src")

	if box.IsNil() { box = sfo.KAirports["KSFO"].Box(250,250) }
	
	as := airspace.NewAirspace()
	var err error

	if src == "" || src == "fdb" {
		as,err = getAirspaceForDisplay(c, box)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	} else if src == "aex" {
		addAExToAirspace(c, box, &as)

	} else {
		http.Error(w, fmt.Sprintf("airspace source '%s' not in {fdb,aex}"), http.StatusBadRequest)
		return
	}	

	if r.FormValue("comp") != "" {
		addAExToAirspace(c, box, &as)
		if r.FormValue("fr24") != "" { addFr24ToAirspace(c, &as) }
		//if r.FormValue("fa") != "" { faToAirspace(c, &as) }

		// Weed out stale stuff (mostly from fa)
		for k,_ := range as.Aircraft {
			age := time.Since(as.Aircraft[k].Msg.GeneratedTimestampUTC)
			if age > kMaxStaleDuration * 2 {
				delete(as.Aircraft, k)
			}
		}
	}
	
	// Bodge, to let goapp serve'd things call the deployed version of this URL
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers",
		"Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	data,err := json.MarshalIndent(as, "", "  ")
	if err != nil {
		http.Error(w, fmt.Sprintf("jOH/Marshal error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// }}}

// {{{ BackfillReferenceData

func BackfillReferenceData(ctx context.Context, as *airspace.Airspace) {

	airframes := ref.NewAirframeCache(ctx)
	schedules := ref.NewScheduleCache(ctx)

	for k,aircraft := range as.Aircraft {
		// Need the bare Icao for lookups
		unprefixedK := strings.TrimPrefix(string(k), "QQ")
		unprefixedK = strings.TrimPrefix(unprefixedK, "EE")
		unprefixedK = strings.TrimPrefix(unprefixedK, "FF")

		if af := airframes.Get(unprefixedK); af != nil {
			// Update entry in map to include the airframe data we just found
			aircraft.Airframe = *af
			as.Aircraft[k] = aircraft
		}

		if schedules != nil && time.Since(schedules.LastUpdated) < kMaxStaleScheduleDuration {
			if fs := schedules.Get(unprefixedK); fs != nil {
				aircraft.Schedule = fs.Identity.Schedule
				as.Aircraft[k] = aircraft
			}
		}
	}
}

// }}}

// {{{ getAirspaceForDisplay

// We tart it up with airframe and schedule data, trim out stale entries, and trim to fit box
func getAirspaceForDisplay(c context.Context, bbox geo.LatlongBox) (airspace.Airspace, error) {
	a := airspace.NewAirspace()

	// Must wait until GAE can securely call into GCE within the same project
/*
	dialer := func(network, addr string, timeout time.Duration) (net.Conn, error) {
		return socket.DialTimeout(c, network, addr, timeout)
	}
	if err := a.JustAircraftFromMemcacheServer(c, dialer); err != nil {
		return a, fmt.Errorf("gAFD/FromMemcache error:%v", err)
	}
*/

	if err := a.JustAircraftFromMemcache(c); err != nil {
		return a, fmt.Errorf("gAFD/FromMemcache error:%v", err)
	}
	
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
	}

	BackfillReferenceData(c, &a)
	
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
// {{{ addAExAirspace

func addAExToAirspace(ctx context.Context, box geo.LatlongBox, as *airspace.Airspace) {	
	if asAEx,err := aex.FetchAirspace(urlfetch.Client(ctx), box); err != nil {
		return
	} else {
		BackfillReferenceData(ctx, asAEx)

		for k,ad := range asAEx.Aircraft {
			newk := string(k)
			newk = "QQ" + strings.TrimPrefix(newk, "QQ") // Remove (if present), then re-add
			ad.Airframe.Icao24 = newk
			as.Aircraft[adsb.IcaoId(newk)] = ad
		}
	}
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
