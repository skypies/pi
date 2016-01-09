// go test -v github.com/skypies/pi/airspace
package airspace

import (
	"bufio"
	//"fmt"
	"strings"
	"testing"
	"github.com/skypies/adsb"
)
	
var (
	// These are all formatted as SBS, but FWIW they're not legal instances of "MSG,3"; they contain all the fields
	bank1 = `
MSG,3,1,1,A81BD0,1,2015/12/25,08:00:00.111111,2015/12/25,08:00:00.111999,ABC1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD1,1,2015/12/25,08:00:00.222222,2015/12/25,08:00:00.222999,DEF1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD2,1,2015/12/25,08:00:00.333333,2015/12/25,08:00:00.333999,GHI1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD3,1,2015/12/25,08:00:00.444444,2015/12/25,08:00:00.444999,JKL1234,36000,300,10,36.69804,-121.86007,+64,,,,,0`

	// Same content as bank1, but with different timestamps
	bank2 = `
MSG,3,1,1,A81BD0,1,2015/12/25,08:00:01.111111,2015/12/25,08:00:01.111999,ABC1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD1,1,2015/12/25,08:00:01.222222,2015/12/25,08:00:01.222999,DEF1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD2,1,2015/12/25,08:00:01.333333,2015/12/25,08:00:01.333999,GHI1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD3,1,2015/12/25,08:00:01.444444,2015/12/25,08:00:01.444999,JKL1234,36000,300,10,36.69804,-121.86007,+64,,,,,0`

	// Contains two dupes, and two actual updates (position changes)
	bank3 = `
MSG,3,1,1,A81BD0,1,2015/12/25,08:00:02.111111,2015/12/25,08:00:03.111999,ABC1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD1,1,2015/12/25,08:00:02.222222,2015/12/25,08:00:03.222999,DEF1234,36000,300,10,36.69804,-121.86007,+64,,,,,0
MSG,3,1,1,A81BD2,1,2015/12/25,08:00:02.333333,2015/12/25,08:00:03.333999,GHI1234,36000,300,10,36.69999,-121.86999,+64,,,,,0
MSG,3,1,1,A81BD3,1,2015/12/25,08:00:02.444444,2015/12/25,08:00:03.444999,JKL1234,36000,305,10,36.69804,-121.86999,+64,,,,,0`
)

// Parse up the SBS strings, and then pretend we've fleshed them out with data into CompositeMsgs
func msgs(sbsText string) (ret []*adsb.CompositeMsg) {
	scanner := bufio.NewScanner(strings.NewReader(sbsText))
	for scanner.Scan() {
		if text := scanner.Text(); text != "" {
			m := adsb.Msg{}
			if err := m.FromSBS1(text); err != nil { panic(err) }
			cm := adsb.CompositeMsg{Msg:m}
			ret = append(ret, &cm)
		}
	}
	return
}

func TestDeduping(t *testing.T) {
	a := Airspace{}

	msgs1 := msgs(bank1)
	if new := a.MaybeUpdate(msgs1); len(new) != len(msgs1) {
		t.Errorf("Initial population: expected %d new, got %d", len(msgs1), len(new))
	}
	
	msgs2 := msgs(bank2)
	if new := a.MaybeUpdate(msgs2); len(new) != 0 {
		t.Errorf("Repopulation of init msgs: expected 0 new, got %d", len(new))
	}
}

func TestPartialDeduping(t *testing.T) {
	a := Airspace{}

	msgs1 := msgs(bank1)
	if new := a.MaybeUpdate(msgs1); len(new) != len(msgs1) {
		t.Errorf("Initial population: expected %d new, got %d", len(msgs1), len(new))
	}
	
	msgs3 := msgs(bank3)
	if new := a.MaybeUpdate(msgs3); len(new) != 2 {
		t.Errorf("Repopulation of init msgs: expected 2 new, got %d", len(new))
	}
	//fmt.Printf("%s", a)
}

func TestDedupingViaPrev(t *testing.T) {
	a := Airspace{}

	msgs1 := msgs(bank1)
	if new := a.MaybeUpdate(msgs1); len(new) != len(msgs1) {
		t.Errorf("Initial population: expected %d new, got %d", len(msgs1), len(new))
	}

	a.rollMsgs()
	
	if new := a.MaybeUpdate(msgs1); len(new) != 0 {
		t.Errorf("Repopulation of init msgs: expected 0 new, got %d", len(new))
	}
}


func TestCompleteAgeout(t *testing.T) {
	a := Airspace{}

	msgs1 := msgs(bank1)
	if new := a.MaybeUpdate(msgs1); len(new) != len(msgs1) {
		t.Errorf("Initial population: expected %d new, got %d", len(msgs1), len(new))
	}

	a.rollMsgs()
	a.rollMsgs()
	
	if new := a.MaybeUpdate(msgs1); len(new) != len(msgs1) {
		t.Errorf("Repopulation of init msgs: expected %d new, got %d", len(msgs1), len(new))
	}
}
