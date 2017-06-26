package date

// go test github.com/skypies/util/date

import "testing"
import "time"

// Parse unzoned time as if it were Pacific Time, and return a zoned time object
// Automatically decides whether to apply PDT or PST, based on the date in the string.
func pdtParse(s string) time.Time {
	pdt,_ := time.LoadLocation("America/Los_Angeles")
	t,_ := time.ParseInLocation("02 Jan 06 15:04", s, pdt)
	return t
}
func pdtParseSecs(s string) time.Time {
	pdt,_ := time.LoadLocation("America/Los_Angeles")
	t,_ := time.ParseInLocation("02 Jan 06 15:04:05", s, pdt)
	return t
}

func nzone(t time.Time) int {
	_,n := t.Zone(); return n
}

func TestTime2Datestring(t *testing.T) {
	datestrings := []string{"2015.10.08", "2016.02.29"}

	for _,datestring := range datestrings {
		s := Time2Datestring(Datestring2MidnightPdt(datestring))
		if s != datestring { t.Errorf("%s ended up as %s", datestring, s) }

		t1 := Datestring2MidnightPdt(datestring)
		if ! t1.Equal(AtLocalMidnight(t1)) { t.Errorf("t1 not at local midnight [%s]", t1) }
	}
}

func TestAtLocalMidnight(t *testing.T) {
	datestrings := []string{"04 Sep 15 20:17", "30 Dec 15 00:00"}

	for _,datestring := range datestrings {
		in := pdtParse(datestring)
		if nzone(in) == 0 {
			t.Errorf("pdtParse giving unzoned: %s", in)		
		}
	
		out := AtLocalMidnight(in)
		if out.Minute() != 0 || out.Hour() != 0 {
			t.Errorf("hours and minutes not rounded off: %s", out)
		}

		if nzone(out) != nzone(in) {
			t.Errorf("Timezones lost: in=%s, out=%s", in, out)
		}
	}
}

func TestWindowForTime(t *testing.T) {
	datestrings := []string{"04 Sep 15 20:17", "30 Dec 15 00:00"}

	for _,datestring := range datestrings {
		in := pdtParse(datestring)
		s,e := WindowForTime(in)

		if nzone(s) != nzone(in) || nzone(e) != nzone(in) {
			t.Errorf("Timezones lost: in=%s, s=%s, e=%s", in, s, e)
		}

		if s.After(in)  { t.Errorf("start was after input: in=%s, s=%s", in, s) }
		if e.Before(in) { t.Errorf("end was before input: in=%s, e=%s", in, e) }
		if e.Before(s)  { t.Errorf("end before start: e=%s, s=%s", e, s) }

		if s.Minute() != 0 || s.Hour() != 0 {
			t.Errorf("hours and minutes not rounded off for start: %s", s)
		}
		if e.Sub(s) != 24 * time.Hour {
			t.Errorf("e is not precisely 24h after s: s=%s, e=%s", s, e)
		}
	}
}

type IMTest struct {
	s,e string
	n   int
}

func TestIntermediateMidnights(t *testing.T) {
	tests := []IMTest{
		{"04 Sep 15 18:17", "04 Sep 15 20:17", 0},  // Same day
		{"04 Sep 15 00:00", "04 Sep 15 00:00", 0},  // Same instant
		{"03 Sep 15 00:00", "04 Sep 15 00:00", 0},  // Nothing strictly intermediate
		{"03 Sep 15 13:00", "04 Sep 15 13:00", 1},  // Midnight of the 3rd/4th
		{"02 Sep 15 13:00", "04 Sep 15 13:00", 2},  // Midnights of the 2nd/3rd & 3rd/4th
		{"02 Sep 15 00:00", "04 Sep 15 00:01", 2},  // Ditto: midnights of the 2nd/3rd & 3rd/4th
		{"02 Sep 15 00:00", "04 Sep 15 00:00", 1},  // Not ditto: lose the second, as it matches end val
		{"30 Dec 15 00:00", "02 Jan 16 00:00", 2},   // Midnights of the Dec 31st & Jan 1st
		{"30 Dec 16 00:00", "02 Jan 17 00:00", 2},   // And again, for a leap year
		{"10 Oct 16 00:00", "15 Nov 16 00:00", 35},   // While the clocks go back ...
		{"15 Apr 15 13:00", "15 May 15 13:00", 30},  // A whole month (but april has 30 days)
		//{"31 Oct 15 17:00", "02 Nov 15 17:00", 2},  // DST again (bah)
	}

	for i,test := range tests {
		im := IntermediateMidnights(pdtParse(test.s), pdtParse(test.e))
		if len(im) != test.n {
			t.Errorf("[t%d] got %d i.m.'s (expected %d)", i, len(im), test.n)
		}

		for j,midnight := range im {
			if !midnight.Equal(AtLocalMidnight(midnight)) {
				t.Errorf("[t%d,%d] i.m. not actually midnight; was %s", i, j, midnight)
			}
			if !(midnight.After(pdtParse(test.s)) && midnight.Before(pdtParse(test.e))) {
				t.Errorf("[t%d,%d] i.m. not strictly inbetween s & e: was %s", i, j, midnight)
			}
			if j>0 && !midnight.After(im[j-1]) {
				t.Errorf("[t%d,%d] i.m. not strictly inbetween s & e: was %s", i, j, midnight)			}
		}
	}
}

/*
func TestIntermediateMidnightsDaylightSavingsTime(t *testing.T) {
	test := IMTest{	"30 Oct 15 11:00", "07 Nov 15 13:00", 3 }
	im := IntermediateMidnights(pdtParse(test.s), pdtParse(test.e))
	t.Errorf("Start=[%s], End=[%s]", pdtParse(test.s), pdtParse(test.e))
	for _,v := range im {
		t.Errorf("I don't like it: [%s]", v)
	}
}
*/

type TimeslotTest struct {
	s,e,d string
	ret []string
}

func TestTimeslots(t *testing.T) {
	tests := []TimeslotTest{
		{"01 Jan 16 11:09:12", "01 Jan 16 11:09:12", "30m",
			[]string{ "01 Jan 16 11:00:00" } },
		{"01 Jan 16 11:09:12", "01 Jan 16 11:29:59", "30m",
			[]string{ "01 Jan 16 11:00:00" } },
		{"01 Jan 16 11:29:59", "01 Jan 16 11:30:00", "30m",
			[]string{ "01 Jan 16 11:00:00",  "01 Jan 16 11:30:00" } },
		{"01 Jan 16 11:09:12", "01 Jan 16 12:15:00", "30m",
			[]string{ "01 Jan 16 11:00:00",  "01 Jan 16 11:30:00", "01 Jan 16 12:00:00" } },
		{"01 Jan 16 23:55:00", "02 Jan 16 00:15:00", "30m",
			[]string{ "01 Jan 16 23:30:00",  "02 Jan 16 00:00:00" } },
	}

	for i,test := range tests {
		s,e := pdtParseSecs(test.s), pdtParseSecs(test.e)
		d,_ := time.ParseDuration(test.d)
		ret := Timeslots(s,e,d)
		if len(ret) != len(test.ret) {
			t.Errorf("[t%d] got %d, expected %d", i, len(ret), len(test.ret))
		} else {
			for i,actual := range ret {
				expected := pdtParseSecs(test.ret[i])
				if !actual.Equal(expected) {
					t.Errorf("[t%d] got %v, expected %v", i, actual, expected)
				}
			}
		}
	}
}
