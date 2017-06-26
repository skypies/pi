package date

import "fmt"
import "time"

const dateNoTimeFormat = "2006.01.02"

var localTimeLocation *time.Location

func init() {
	// time.LoadLocation costs ~1ms, so do it just once and cache in a global
	localTimeLocation,_ = time.LoadLocation("America/Los_Angeles")
}

// All these FooPdt functions should be renamed FooPacificTime; they're not specific to
// dayight savings. And in fact should be turned into LocalTime or something.
func InPdt(t time.Time) time.Time {
	return t.In(localTimeLocation)
}

func NowInPdt() time.Time {	return InPdt(time.Now()) }

func ParseInPdt(format string, value string) (time.Time, error) {
	t,err := time.ParseInLocation(format, value, localTimeLocation)
	if err != nil { return NowInPdt(), err }
	return t, nil
}

func Time2Datestring(t time.Time) string {
	return InPdt(t).Format(dateNoTimeFormat)
}

func ArbitraryDatestring2MidnightPdt(s string, fmt string) time.Time {
	t,_ := time.ParseInLocation(fmt, s, localTimeLocation)
	return t
}

func Datestring2MidnightPdt(s string) time.Time {
	return ArbitraryDatestring2MidnightPdt(s, dateNoTimeFormat)
}

// Legacy name, should fixup.
func AtLocalMidnight(in time.Time) time.Time { return TruncateToLocalDay(in) }

// Round off to 00:00:00 today (i.e. most recent midnight)
// Can't use time.Round(24*time.Hour); it operates in UTC, so rounds into weird boundaries
func TruncateToLocalDay(in time.Time) time.Time {
	return time.Date(in.Year(), in.Month(), in.Day(), 0, 0, 0, 0, in.Location())
}
// Perhaps time.Round(24*time.Hour) works here ?
func TruncateToUTCDay(in time.Time) time.Time {
	t := in.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}


// A 'window' is a pair of times spanning 23-25h, respecting the timezone of the input, and
// honoring daylight savings changes.
// start will be midnight (00:00:00) that day; end is 24h later, i.e. 00:00:00 the next day
func WindowForTime(t time.Time) (start time.Time, end time.Time) {
	start = AtLocalMidnight(t)
	end = start.AddDate(0,0,1)
	return
}

// Get the window for all in the seconds in the month which contains the time
func MonthWindowForTime(t time.Time) (start time.Time, end time.Time) {
	ref := InPdt(t)
	start = time.Date(ref.Year(), ref.Month(), 1, 0,0,0,0, ref.Location()) // first second of month
	end = start.AddDate(0,1,0).Add(-1 * time.Second) // one month forward, one second back
	return
}


// Return all midnights inbetween s & e - not including s or e if they happen to be midnight.
// Caller should ensure s and e are in the same timezone. 
func IntermediateMidnights(s,e time.Time) (ret []time.Time) {
	if s.Equal(e)  { return }
	if !e.After(s) { panic(fmt.Sprintf("IntermediateMidnights: start>end: %s, %s", s, e)) }

	_, m := WindowForTime(s)  // init: get first midnight that follows s
	for m.Before(e) {
		ret = append(ret, m)
		m = m.AddDate(0,0,1)  // This works cleanly through Daylight Savings transitions.
	}

	return
}

// Convenience helpers
func WindowForToday() (start time.Time, end time.Time) {
	return WindowForTime(NowInPdt())
}

func WindowForYesterday() (start time.Time, end time.Time) {
	return WindowForTime(NowInPdt().AddDate(0,0,-1))
}

// Returns a list of time buckets, of given duration, that the inputs span. Each bucket is
// returned as the time-instant that begins the bucket.
func Timeslots(s,e time.Time, d time.Duration) []time.Time {
	ret := []time.Time{}

	// time.Round rounds to nearest (rounds up as tiebreaker), so subtract half to ensure round down
	for t := s.Add(-1 * d/2).Round(d); !t.After(e); t = t.Add(d) {
		ret = append(ret, t)
	}

	return ret
}

func RoundDuration(d time.Duration) time.Duration {
	return time.Duration(int64(d.Seconds())) * time.Second
}

func DateRangeToPacificTimeWindows(sStr,eStr string) [][]time.Time {
	start := ArbitraryDatestring2MidnightPdt(sStr, "2006/01/02").Add(-1 * time.Second)
	end   := ArbitraryDatestring2MidnightPdt(eStr, "2006/01/02").Add(1 * time.Second)

	ret := [][]time.Time{}
	
	for _,day := range IntermediateMidnights(start,end) {
		s,e := WindowForTime(day)
		e = e.Add(-1 * time.Second)
		ret = append(ret, []time.Time{s,e})
	}

	return ret
}
