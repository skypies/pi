package airspace

import (
	"fmt"
	"html/template"
	"time"
)

func (ad AircraftData)ToJSString(host string) string {
	m := ad.Msg	
	idSpec := fmt.Sprintf("%s@%d", string(m.Icao24), time.Now().Add(-30 * time.Second).Unix())

	//host := "ui-dot-serfr0-fdb.appspot.com"
	//host := "fdb.serfr1.org"
	//fdbUrl      := fmt.Sprintf("http://%s/fdb/tracks?idspec=%s", host, idSpec)
	//approachUrl := fmt.Sprintf("http://%s/fdb/approach?idspec=%s", host, idSpec)

	fdbUrl      := fmt.Sprintf("/fdb/tracks?idspec=%s", idSpec)
	approachUrl := fmt.Sprintf("/fdb/descent?idspec=%s", idSpec)
	if host == "airspace.serfr1.org" { fdbUrl,approachUrl = "","" } // Bleargh

	faUrl := fmt.Sprintf("http://flightaware.com/live/flight/%s", m.Callsign)
	
	color := "#0033ff"
	if m.DataSystem() == "MLAT" { color = "#508aff" }

	if ad.Source == "fa" { color = "#ff3300" }
	if ad.Source == "fr24" { color = "#00ff33" }

	return fmt.Sprintf("receiver:\"%s\", callsign:%q, icao24:%q, reg:%q, equip:%q, icao:%q, "+
		"flightnumber:%q, orig:%q, dest:%q, " +	
		"pos:{lat:%.6f,lng:%.6f}, alt:%d, heading:%d, speed:%d, vertspeed:%d, "+
		"squawk:%q, faurl:%q, fdburl:%q, approachurl:%q, idspec:%q, age:%.0f, source:%q, system:%q, "+
		"color:%q",
		m.ReceiverName, m.Callsign, m.Icao24, ad.Registration, ad.EquipmentType, ad.CallsignPrefix,
		ad.Schedule.BestFlightNumber(), ad.Schedule.Origin, ad.Schedule.Destination,
		m.Position.Lat, m.Position.Long, m.Altitude, m.Track, m.GroundSpeed, m.VerticalRate,
		m.Squawk, faUrl, fdbUrl, approachUrl, idSpec,
		time.Since(m.GeneratedTimestampUTC).Seconds(),
		ad.Source, m.DataSystem(), color)
}

func (a *Airspace)ToJSVar(host string) template.JS {
	str := "{\n"
	for i,ad := range a.Aircraft {
		str += fmt.Sprintf("    %q: {%s},\n", i, ad.ToJSString(host))
	}
	str += "  }"
	return template.JS(str)
}
