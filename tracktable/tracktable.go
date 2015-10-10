package tracktable

import (
	"fmt"
	//"io/ioutil"
	"net/http"
	"net/url"
	"time"
	"github.com/skypies/adsb"
	"github.com/skypies/flightdb"
)

type TrackTable struct {
	StationName string
	PostURL     string // host/path, where to push finished tracks to
	WaitTime    int64  // time.Seconds
	Tracks      map[adsb.IcaoId]*Track
}

type Track struct {
	updateTime   time.Time // When did this track last get an upadte
	Icao         adsb.IcaoId
	Messages  []*adsb.Msg

	// These values are used to cache data fields that don't show up in all messages
	// We only store messages that contain position data; but we 'top them up' with these values
	LastGroundSpeed   int64
	LastVerticalSpeed int64
	LastTrack         int64
	LastCallsign      string
}

func  (t TrackTable)String() string {
	s := ""
	for k,v := range t.Tracks {
		s += fmt.Sprintf("[%s] : %d points (first=%s)\n", k, len(v.Messages), v.Messages[0])
	}
	return s
}

func New() TrackTable {
	return TrackTable{
		WaitTime: -1, // Purge as soon as we sweep
		Tracks: map[adsb.IcaoId]*Track{},
	}
}

func (t *TrackTable)AddTrack(icao adsb.IcaoId) {
	track := Track{
		Icao: icao,
		Messages: []*adsb.Msg{},
	}
	//fmt.Printf("Noticed [%s]\n", icao)
	t.Tracks[icao] = &track
}

func (t *TrackTable)RemoveTracks(icaos []adsb.IcaoId) []*Track{
	removed := []*Track{}
	for _,icao := range icaos {
		removed = append(removed, t.Tracks[icao])
		delete(t.Tracks, icao)
	}
	return removed
}

func (t *TrackTable)AddMessage(m *adsb.Msg) {
	if _,exists := t.Tracks[m.Icao24]; exists == false {
		t.AddTrack(m.Icao24)
	}
	track := t.Tracks[m.Icao24]
	track.updateTime = time.Now()

	// These subtype packets have data we don't get in the bulk of position packets (subtype:3),
	// so just cache their interesting data and inject it into next position packet.
	// http://woodair.net/SBS/Article/Barebones42_Socket_Data.htm
	if m.SubType == 1 {
		track.LastCallsign = m.Callsign
		
	} else if m.SubType == 4 {
		track.LastGroundSpeed   = m.GroundSpeed
		track.LastVerticalSpeed = m.VerticalRate
		track.LastTrack         = m.Track

	} else if m.SubType == 3 {
		// Inject cached values into this position packet
		m.Callsign     = track.LastCallsign   
		m.GroundSpeed  = track.LastGroundSpeed   
		m.VerticalRate = track.LastVerticalSpeed
		m.Track        = track.LastTrack
	}

	// Only persist this message if it has new position information; that will be subtypes 2 & 3.
	if m.HasPosition() {
		track.Messages = append(track.Messages, m)
	}
}

func (t TrackTable)ListFinished() []adsb.IcaoId {
	// IS this thread-safe ?
	ret := []adsb.IcaoId{}
	for icao,track := range t.Tracks {
		if time.Since(track.updateTime) > (time.Duration(t.WaitTime) * time.Second) {
			ret = append(ret, icao)
		}
	}
	return ret
}

func (t *TrackTable)Sweep() {
	ready := t.ListFinished()
	removed := t.RemoveTracks(ready)
	// Spin this work off into a thread, so we can get back to handling packets
	go SendTracks(removed)
}

func SendTracks(tracks []*Track) {
	for _,track := range tracks {
		if len(track.Messages) > 0 {
			// fmt.Printf("Purging [%s] %d points\n", track.Icao, len(track.Messages))
			track.Send()
		}
	}
}

//////////////////////////////////////////////

func adsbToTrackPoint(m *adsb.Msg) flightdb.TrackPoint {
	tp := flightdb.TrackPoint{
		Source: "ADSB",
		Station: "StationName",  // Needs to come from a command line variable
		TimestampUTC: m.GeneratedTimestampUTC,
		Heading: float64(m.Track), // Direction of travel, not direction pointed in; badly named
		Latlong: m.Position,
		SpeedKnots: float64(m.GroundSpeed),
		AltitudeFeet: float64(m.Altitude),
		Squawk: m.Squawk,
		VerticalRate:  float64(m.VerticalRate),
	}
	return tp
}

func (track Track)ToFlightDBTrack() flightdb.Track {
	out := flightdb.Track{}
	for _,m := range track.Messages {
		out = append(out, adsbToTrackPoint(m))
	}
	return out
}

func (track Track)Send() {
	host := "complaints.serfr1.org" //"localhost:8080"
	path := "/fdb/addtrack"

	outTrack := track.ToFlightDBTrack()
	outBase64,_ := outTrack.Base64Encode()

	//return // XXX DISABLED FOR SAFETY
	
	resp,err := http.PostForm("http://"+host+path, url.Values{
		"icaoid": {string(track.Icao)},
		"callsign": {string(track.LastCallsign)},
		"track": {outBase64},
	})

	if err != nil {
		//fmt.Printf("Oh noes: %v\n", err)

	} else {
		defer resp.Body.Close()
		//body,_ := ioutil.ReadAll(resp.Body)		
		//fmt.Printf(" ----> [%s] %s:-\n%s", track.Icao, track.LastCallsign, body)
	}
}
