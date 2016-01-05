// Package airspace maintains a single, central snapshot of current aircraft status
package airspace

import(
	"fmt"
	"sort"
	"time"

	"github.com/skypies/adsb"
)

var	DefaultRollAfter = time.Minute * 5
var DefaultMaxQuietTime = time.Minute * 5

type AircraftData struct {
	Msg *adsb.CompositeMsg
}

type Airspace struct {
	//// Data for message deduping.
	// We roll curr into prev (and lose the old prev) as per rollAfter; this is to
	// keep the memory footprint under control.
	currMsgs         map[adsb.Signature]bool
	prevMsgs         map[adsb.Signature]bool
	rollAfter        time.Duration
	timeOfLastRoll   time.Time

	//// Data to represent "what is in the sky right now".
	//
	Aircraft map[adsb.IcaoId]AircraftData
}

// {{{ a.rollMsgs

func (a *Airspace)rollMsgs() {
	a.prevMsgs = a.currMsgs
	a.currMsgs = make(map[adsb.Signature]bool)
	a.timeOfLastRoll = time.Now()

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
	if a.rollAfter == time.Minute * 0 { a.rollAfter = DefaultRollAfter }
	if a.Aircraft == nil { a.Aircraft = make(map[adsb.IcaoId]AircraftData) }
	
	sig := msg.GetSignature()
	if _,existsCurr := a.currMsgs[sig]; !existsCurr {
		// Add it into Curr in all cases
		a.currMsgs[sig] = true

		existsPrev := false
		if a.prevMsgs != nil { _,existsPrev = a.prevMsgs[sig] }

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
		str += fmt.Sprintf(" %7.7s/%s (lastSeen:%7.1fs) : %6dft, %4dknots\n",
			ac.Msg.Callsign, ac.Msg.Icao24,
			time.Since(ac.Msg.GeneratedTimestampUTC).Seconds(),
			ac.Msg.Altitude, ac.Msg.GroundSpeed)
	}
	return str
}

// }}}
// {{{ a.MaybeUpdate

// If any messages are new, update our view of the world. Return the indicies of the messages
// we thought were new.
func (a *Airspace) MaybeUpdate(msgs []*adsb.CompositeMsg) []*adsb.CompositeMsg {
	ret := []*adsb.CompositeMsg{}

	// Time to roll (or lazily init) ?
	if time.Since(a.timeOfLastRoll) > a.rollAfter { a.rollMsgs() }

	for _,msg := range msgs {
		if a.thisIsNewContent(msg) {
			ret = append(ret,msg)
			a.Aircraft[msg.Icao24] = AircraftData{Msg: msg}
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
