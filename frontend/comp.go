package main

// A handler to compare/contrast various airspaces on top of each other

import (
	"fmt"
	"os"
	"regexp"
	"time"
	
	"golang.org/x/net/context"
	"google.golang.org/appengine/urlfetch"

	"github.com/skypies/geo"
	"github.com/skypies/geo/sfo"
	"github.com/skypies/adsb"

	"github.com/skypies/pi/airspace"

	fdb "github.com/skypies/flightdb2"
	"github.com/skypies/flightdb2/fa"
	"github.com/skypies/flightdb2/fr24"
)

func init() {
	//http.HandleFunc("/comp", compHandler)
	//http.HandleFunc("/comp2", compHandler)
}


// {{{ faFlight2AirspaceAircraft

func faFlight2AircraftData(in fa.InFlightStruct, id adsb.IcaoId) airspace.AircraftData {
	msg := adsb.CompositeMsg{
		Msg: adsb.Msg{
			Icao24: id,
			GeneratedTimestampUTC: time.Unix(int64(in.Timestamp),0).UTC(),
			Callsign: in.Ident,
			Altitude: int64(in.Altitude)*100,
			GroundSpeed: int64(in.Groundspeed),
			Track: int64(in.Heading),
			Position: geo.Latlong{in.Latitude, in.Longitude},
		},
		ReceiverName: "FlightAware",
	}

	return airspace.AircraftData{
 		Msg: &msg,

		Airframe: fdb.Airframe{
			Icao24: string(id),
			EquipmentType: in.EquipType,
		},

		NumMessagesSeen: 1,
		Source: "fa",
	}
}

// }}}
// {{{ snapshot2AirspaceAircraft

func snapshot2AircraftData(fs fdb.FlightSnapshot, id adsb.IcaoId) airspace.AircraftData {	
	msg := adsb.CompositeMsg{
		Msg: adsb.Msg{
			Icao24: id,
			GeneratedTimestampUTC: fs.Trackpoint.TimestampUTC,
			Callsign: fs.Flight.NormalizedCallsignString(),
			Altitude: int64(fs.Trackpoint.Altitude),
			GroundSpeed: int64(fs.Trackpoint.GroundSpeed),
			Track: int64(fs.Trackpoint.Heading),
			Position: fs.Trackpoint.Latlong,
		},
		ReceiverName: fs.Trackpoint.ReceiverName,
	}

	af := fs.Flight.Airframe
	af.Icao24 = string(id)

	// Hack up some fake 'message types' ...
	if fs.Trackpoint.DataSource == "fr24" {
		if tf5m := regexp.MustCompile("T-F5M").FindString(msg.ReceiverName); tf5m != "" {
			msg.Type = "T-F5M"
		} else if mlat := regexp.MustCompile("MLAT").FindString(msg.ReceiverName); mlat != "" {
			msg.Type = "MLAT"
		}
	}
	
	return airspace.AircraftData{
 		Msg: &msg,
		Airframe: af,
		NumMessagesSeen: 1,
		Source: fs.Trackpoint.DataSource,
	}
}

// }}}

// {{{ faToAirspace

var(
	TestAPIKey = "foo"
	TestAPIUsername = "bar"
)

// Overlays the flightaware 'Search' results into the airspace
func faToAirspace(c context.Context, as *airspace.Airspace) string {
	str := ""
	
	myFa := fa.Flightaware{APIKey:TestAPIKey, APIUsername:TestAPIUsername, Client:urlfetch.Client(c)}
	myFa.Init()

	box := sfo.KLatlongSFO.Box(250,250)
	// http://flightaware.com/commercial/flightxml/explorer/#op_Search
	//q := "-filter airline -inAir 1 -aboveAltitude 2"	
	q := "-inAir 1"	
	ret,err := myFa.CallSearch(q, box)
	if err != nil { str += fmt.Sprintf("FA/Search() err: %v", err) }
	
	for i,f := range ret {
		id := adsb.IcaoId(fmt.Sprintf("FF%04x", i))
		as.Aircraft[id] = faFlight2AircraftData(f, id)
		str += fmt.Sprintf(" * %s\n", f)
	}
	
	return str
}

// }}}
// {{{ fr24ToAirspace

// Overlays the fr24 current list results into the airspace
func fr24ToAirspace(c context.Context, as *airspace.Airspace) string {
	str := ""

	fr,_ := fr24.NewFr24(urlfetch.Client(c))
	snapshots,err := fr.LookupCurrentList(sfo.KAirports["KSFO"].Box(250,250))
	if err != nil {
		str += fmt.Sprintf("fr24/Current: err: %v\n", err)
		return str
	}
	
	for _,f := range snapshots {
		if f.Altitude < 10 { continue }
		//if f.Destination == "" { continue } // Strip out general aviation
		str += fmt.Sprintf(" * EE %s\n", f)

		id := adsb.IcaoId(fmt.Sprintf("EE%s", f.IcaoId))
		as.Aircraft[id] = snapshot2AircraftData(f,id)
	}
	
	return str
}

// }}}

/*

// {{{ compHandler

func buildLegend() string {
	legend := date.NowInPdt().Format("15:04:05 MST (2006/01/02)")
	return legend
}

// A few conclusions ...
// fr24 is fairly timely; usually only 4s stale
// fa is pretty stel; 20-40s delay
// ... so fa/Search is not super useful for realtime ID :(

// Compare various realtime views of the airspace
func compHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	as,err := getAirspaceForDisplay(c, sfo.KAirports["KSFO"].Box(250,250))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	str := "OK\n\n"
	
	deb := fr24ToAirspace(c, &as)
	str += "**fr24**\n"+deb

	if r.FormValue("fa") != "" {
		deb = faToAirspace(c, &as)
		str += "\n**fa**\n"+deb
	}
	
	str += fmt.Sprintf("\n** * everything ****\n\n%s", as)

	// Weed out stale stuff (mostly from fa)
	for k,_ := range as.Aircraft {
		age := time.Since(as.Aircraft[k].Msg.GeneratedTimestampUTC)
		if age > kMaxStaleDuration * 2 {
			delete(as.Aircraft, k)
		}
	}

	data,err := json.MarshalIndent(as, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	if r.FormValue("text") != "" {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(str))
	} else {
		var params = map[string]interface{}{
			"Legend": buildLegend(),
			"AircraftJSON": template.JS(data),
			"MapsAPIKey": "",
			"Center": sfo.KFixes["YADUT"],
			"Zoom": 9,
		}

		if err := templates.ExecuteTemplate(w, "map-static", params); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// }}}

*/

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
