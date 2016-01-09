package airspace

import (
	"fmt"
	"html/template"
	"time"
)

func (ad AircraftData)ToJSString() string {
	m := ad.Msg	
	fdbUrl := fmt.Sprintf("https://ui-dot-serfr0-fdb.appspot.com/fdb/tracks?icaoid=%s&t=%d",
		string(m.Icao24), time.Now().Add(-30 * time.Second))
	faUrl := fmt.Sprintf("http://flightaware.com/live/flight/%s", m.Callsign)

	return fmt.Sprintf("receiver:\"%s\", callsign:%q, icao24:%q, reg:%q, equip:%q, icao:%q, "+
		"pos:{lat:%.6f,lng:%.6f}, alt:%d, heading:%d, speed:%d, vertspeed:%d, "+
		"squawk:%q, faurl:%q, fdburl:%q, age:%.0f",
		m.ReceiverName, m.Callsign, m.Icao24, ad.Registration, ad.EquipmentType, ad.CallsignPrefix,
		m.Position.Lat, m.Position.Long, m.Altitude, m.Track, m.GroundSpeed, m.VerticalRate,
		m.Squawk, faUrl, fdbUrl,
		time.Since(m.GeneratedTimestampUTC).Seconds())
}

func (a *Airspace)ToJSVar() template.JS {
	str := "{\n"
	for i,ad := range a.Aircraft {
		str += fmt.Sprintf("    %q: {%s},\n", i, ad.ToJSString())
	}
	str += "  }"
	return template.JS(str)
}
