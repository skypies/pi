package airspace

import (
	"fmt"
	"html/template"
	"time"
)

func (ad AircraftData)ToJSString() string {
	m := ad.Msg	
	host := "ui-dot-serfr0-fdb.appspot.com"
	idSpec := fmt.Sprintf("%s@%d", string(m.Icao24), time.Now().Add(-30 * time.Second).Unix())

	fdbUrl      := fmt.Sprintf("https://%s/fdb/tracks?idspec=%s", host, idSpec)
	approachUrl := fmt.Sprintf("https://%s/fdb/approach?idspec=%s", host, idSpec)
	faUrl := fmt.Sprintf("http://flightaware.com/live/flight/%s", m.Callsign)
	
	color := "#0033ff"
	if ad.Source == "fa" { color = "#ff3300" }
	if ad.Source == "fr24" { color = "#00ff33" }
	
	return fmt.Sprintf("receiver:\"%s\", callsign:%q, icao24:%q, reg:%q, equip:%q, icao:%q, "+
		"pos:{lat:%.6f,lng:%.6f}, alt:%d, heading:%d, speed:%d, vertspeed:%d, "+
		"squawk:%q, faurl:%q, fdburl:%q, approachurl:%q, idspec:%q, age:%.0f, source:%q, color:%q",
		m.ReceiverName, m.Callsign, m.Icao24, ad.Registration, ad.EquipmentType, ad.CallsignPrefix,
		m.Position.Lat, m.Position.Long, m.Altitude, m.Track, m.GroundSpeed, m.VerticalRate,
		m.Squawk, faUrl, fdbUrl, approachUrl, idSpec,
		time.Since(m.GeneratedTimestampUTC).Seconds(),
		ad.Source, color)
}

func (a *Airspace)ToJSVar() template.JS {
	str := "{\n"
	for i,ad := range a.Aircraft {
		str += fmt.Sprintf("    %q: {%s},\n", i, ad.ToJSString())
	}
	str += "  }"
	return template.JS(str)
}
