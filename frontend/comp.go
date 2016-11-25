package main

// Generate airspaces from fr24 and flightaware's API, for realtime comparisons.

import (
	"fmt"
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

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
