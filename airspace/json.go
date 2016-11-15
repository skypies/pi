// Package airspace maintains a single, central snapshot of current aircraft status
package airspace

import(
	"encoding/json"
	"fmt"
	"time"
)

type FakeAircraftData AircraftData

// As we write the JSON, add some synthetic fields. We cast our object into a fake datatype
// to avoid infinite regress; this works, as it gets collapsed back into the same set of vals.
func (ad AircraftData) MarshalJSON() ([]byte, error) {
	m := ad.Msg
	idSpec := fmt.Sprintf("%s@%d", string(m.Icao24), time.Now().Unix())

	if ad.Source == "" { ad.Source = "SkyPi" }
	callsign := m.Callsign
	
	return json.Marshal(struct {
		FakeAircraftData

		X_UrlSkypi        string
		X_UrlDescent      string
		X_UrlFA           string
		X_UrlFR24         string

		X_DataSystem      string
		X_AgeSecs         string  // duration string
	}{
		FakeAircraftData: FakeAircraftData(ad),

		X_UrlSkypi: fmt.Sprintf("/fdb/tracks?idspec=%s", idSpec),
		X_UrlDescent: fmt.Sprintf("/fdb/descent?idspec=%s", idSpec),
		X_UrlFA: fmt.Sprintf("http://flightaware.com/live/flight/%s", callsign),
		X_UrlFR24: fmt.Sprintf("http://www.flightradar24.com/%s", callsign),
		X_DataSystem: m.DataSystem(),
		X_AgeSecs: fmt.Sprintf("%.0f", time.Since(m.GeneratedTimestampUTC).Seconds()),
	})
}
