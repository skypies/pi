package airspace

import (
	"fmt"
	"html/template"
	"time"
)

func (ad AircraftData)ToJSString() string {
	m := ad.Msg
	url := fmt.Sprintf("http://flightaware.com/live/flight/%s", m.Callsign)
	return fmt.Sprintf("receiver:\"%s\", callsign:%q, icao24:%q, pos:{lat:%.6f,lng:%.6f}, "+
		"alt:%d, heading:%d, speed:%d, vertspeed:%d, squawk:%q, url:%q, age:%.0f",
		m.ReceiverName, m.Callsign, m.Icao24, m.Position.Lat, m.Position.Long,
		m.Altitude, m.Track, m.GroundSpeed, m.VerticalRate, m.Squawk, url,
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
