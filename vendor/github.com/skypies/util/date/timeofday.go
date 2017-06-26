package date

import "fmt"
import "time"

// TimeOfDayRange specifies a duration within a day
type TimeOfDayRange struct {
	Hour, Minute  int
	Length        time.Duration
}

func (tr TimeOfDayRange)IsInitialized() bool { return tr.Length > 0 }

func (tr TimeOfDayRange)Start() time.Time {
	return time.Date(1970,0,0, tr.Hour,tr.Minute,0,0, time.UTC)
}

func (tr TimeOfDayRange)End() time.Time {
	return tr.Start().Add(tr.Length).Add(-1 * time.Second)	
}

// Given an 'anchor day' t, find the start/end times of the TimeOfDay inside it. If the range
// straddles midnight, then 'end' will come before 'start', and will be the end of the previous
// day's range. Note that 't' *must* have a location defined, or this will panic().
func (tr TimeOfDayRange)AnchorInsideDay(t time.Time) (time.Time,time.Time) {
	s := time.Date(t.Year(), t.Month(), t.Day(), tr.Hour,tr.Minute,0,0, t.Location())
	e := s.Add(tr.Length).Add(-1 * time.Second)

	if e.Hour() < s.Hour() {
		// We have straddled midnight ! End will be in tomorrow; bring back
		e = e.AddDate(0,0,-1)
	}

	return s,e
}
	
func ParseTimeOfDay(start string, length string) (TimeOfDayRange, error) {
	ret := TimeOfDayRange{}
	if t,err := time.Parse("15:04", start); err != nil {
		return ret, fmt.Errorf("ParseTimeOfDay(%s,%s) start: %v", start, length, err)
	} else if d,err := time.ParseDuration(length); err != nil {
		return ret, fmt.Errorf("ParseTimeOfDay(%s,%s) length: %v", start, length, err)
	} else if d > (23 * time.Hour) {
		return ret, fmt.Errorf("ParseTimeOfDay(%s,%s) length too long (23h max)", start, length)
	} else {
		return TimeOfDayRange{Hour:t.Hour(), Minute:t.Minute(), Length:d}, nil
	}
}

func (tr TimeOfDayRange)String() string {
	return fmt.Sprintf("%02d:%02d+%s", tr.Hour, tr.Minute, tr.Length)
}

func (tr TimeOfDayRange)Contains(t time.Time) bool {
	s,e := tr.AnchorInsideDay(t)

	if e.Hour() < s.Hour() {
		// We straddle midnight, so compare against two ranges: [s,23:59] and [00:00,e]
		return t.Before(e)   ||   (t.Equal(s) || t.After(s))	
	}

	// The range doesn't straddle midnight, so t must be in the range [s,e]
	return (t.Equal(s) || t.After(s)) && t.Before(e)
}

func (tr TimeOfDayRange)Overlaps(s,e time.Time) bool {
	// Implement me.
	return false
}

