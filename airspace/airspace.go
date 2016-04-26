// Package airspace maintains a single, central snapshot of current aircraft status
package airspace

import(
	"fmt"
	"sort"
	"time"

	"github.com/skypies/adsb"
	fdb "github.com/skypies/flightdb2"
)

var	DefaultRollAfter = time.Minute * 5
var DefaultMaxQuietTime = time.Minute * 5

type AircraftData struct {
	Msg *adsb.CompositeMsg
	fdb.Airframe // We might get this from an airframe lookup
	fdb.Schedule // We might get this from a schedule lookup
	NumMessagesSeen int64
	Source string // Where this data was sourced
}

type Signatures struct {
	//// Data for message deduping.
	// We roll curr into prev (and lose the old prev) as per rollAfter; this is to
	// keep the memory footprint under control.
	CurrMsgs         map[adsb.Signature]bool
	PrevMsgs         map[adsb.Signature]bool
	RollAfter        time.Duration
	TimeOfLastRoll   time.Time
}
	
type Airspace struct {
	Signatures `json:"-"`                  // What we've seen "recently"; for deduping
	Aircraft map[adsb.IcaoId]AircraftData  // "what is in the sky right now"; for realtime serving
}

// {{{ a.rollMsgs

func (a *Airspace)rollMsgs() {
	a.PrevMsgs = a.CurrMsgs
	a.CurrMsgs = make(map[adsb.Signature]bool)
	a.TimeOfLastRoll = time.Now()

	// Perhaps this should happen elsewhere, but hey, here works.
	for k,_ := range a.Aircraft {
		age := time.Since(a.Aircraft[k].Msg.GeneratedTimestampUTC)
		if age > DefaultMaxQuietTime {
			delete(a.Aircraft, k)
		}
	}
}

// }}}
// {{{ a.thisIsNewContent

func (a *Airspace)thisIsNewContent(msg *adsb.CompositeMsg) (wasNew bool) {
	// Lazy init 
	if a.RollAfter == time.Minute * 0 { a.RollAfter = DefaultRollAfter }
	if a.Aircraft == nil { a.Aircraft = make(map[adsb.IcaoId]AircraftData) }
	
	sig := msg.GetSignature()
	if _,existsCurr := a.CurrMsgs[sig]; !existsCurr {
		// Add it into Curr in all cases
		a.CurrMsgs[sig] = true

		existsPrev := false
		if a.PrevMsgs != nil { _,existsPrev = a.PrevMsgs[sig] }

		// If the thing was already in prev, then it isn't new; else it is
		return !existsPrev
	}

	return false
}

// }}}

// {{{ a.String

func (a Airspace)String() string {
	str := ""

	keys := []string{}
	for k,_ := range a.Aircraft { keys = append(keys, string(k)) }
	sort.Strings(keys)
	
	for _,k := range keys {
		ac := a.Aircraft[adsb.IcaoId(k)]
		str += fmt.Sprintf(" %8.8s/%s/%s (%s last:%6.1fs, %5d msgs) %5df, %3dk\n",
			ac.Msg.Callsign, ac.Msg.Icao24, ac.Registration,
			ac.Msg.DataSystem(),
			time.Since(ac.Msg.GeneratedTimestampUTC).Seconds(), ac.NumMessagesSeen,
			ac.Msg.Altitude, ac.Msg.GroundSpeed)
	}
	return str
}

// }}}
// {{{ a.Youngest

func (a Airspace)Youngest() time.Duration {
	youngest := time.Hour * 480
	for _,ad := range a.Aircraft {
		age := time.Since(ad.Msg.GeneratedTimestampUTC)
		if age < youngest { youngest = age }
	}
	return youngest
}

// }}}

// {{{ a.MaybeUpdate

// If any messages are new, update our view of the world. Return the indicies of the messages
// we thought were new.
func (a *Airspace) MaybeUpdate(msgs []*adsb.CompositeMsg) []*adsb.CompositeMsg {
	ret := []*adsb.CompositeMsg{}

	// Time to roll (or lazily init) ?
	if time.Since(a.TimeOfLastRoll) > a.RollAfter { a.rollMsgs() }

	for _,msg := range msgs {
		if a.thisIsNewContent(msg) {
			numMsg := int64(0)
			if _,exists := a.Aircraft[msg.Icao24]; exists==true {
				numMsg = a.Aircraft[msg.Icao24].NumMessagesSeen
			}
			ret = append(ret,msg)
			a.Aircraft[msg.Icao24] = AircraftData{Msg: msg, NumMessagesSeen: int64(numMsg+1)}
		}
	}
	
	return ret
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
