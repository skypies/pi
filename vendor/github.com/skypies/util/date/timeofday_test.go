package date

// go test github.com/skypies/util/date

import (
	"testing"
	"time"
)

func TestBasics(t *testing.T) {
	tests := []struct {
		S,D string
		Expected bool
	}{
		{"09:25", "30m", false},
		{"22:00", "1h59m", false},
		{"22:00", "2h", false},
		{"22:00", "2h1m", true},
		{"23:50", "2h1m", true},
	}

	straddlesMidnight := func (tr TimeOfDayRange) bool {
		s,e := tr.AnchorInsideDay(time.Now().UTC())
		return e.Hour() < s.Hour()
	}

	for i,test := range tests {
		tr,err := ParseTimeOfDay(test.S, test.D)
		if err != nil { t.Errorf("%d: %v\n", i, err) }
		actual := straddlesMidnight(tr)
		if actual != test.Expected {
			t.Errorf("%d: actual=%v, expected=%v\n * tr=%s\n", i, actual, test.Expected, tr)
		}
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		S,D,T string
		Expected bool
	}{

		// Boundary tests; should be [9:25,9:55)
		{"09:25", "30m", "09:24 +0000", false},
		{"09:25", "30m", "09:25 +0000", true},
		{"09:25", "30m", "09:50 +0000", true},
		{"09:25", "30m", "09:54 +0000", true},
		{"09:25", "30m", "09:55 +0000", false},
		{"09:25", "30m", "09:56 +0000", false},

		// Over midnight tests
		{"23:45", "30m", "23:30 +0000", false},
		{"23:45", "30m", "23:50 +0000", true},
		{"23:45", "30m", "00:10 +0000", true},
		{"23:45", "30m", "00:20 +0000", false},
		{"23:45", "30m", "10:20 +0000", false},

		// Timezone tests
		{"16:00", "2h", "15:59 -1000", false},
		{"16:00", "2h", "16:00 -1000", true},
		{"16:00", "2h", "17:59 -1000", true},
		{"16:00", "2h", "18:01 -1000", false},

	}

	parseTime := func (s string) time.Time {
		tm,err := time.Parse("15:04 -0700", s)
		if err != nil { t.Errorf("parsetime '%s' : %v", s, err) }
		//t = t.UTC().AddDate(2012, 3, 3)
		return tm
	}
	
	for i,test := range tests {
		tr,err := ParseTimeOfDay(test.S, test.D)
		tm := parseTime(test.T)
		if err != nil { t.Errorf("%d: %v\n", i, err) }

		actual,deb := tr.Contains(tm),""
		if actual != test.Expected {
			t.Errorf("%d: actual=%v, expected=%v\n * tr=%s\n * tm=%s\n%s", i, actual, test.Expected, tr,
				tm.UTC(),deb)
		}
		//fmt.Printf("** %s: %s\n", tr, tr.ToCGIArgs("asds"))
	}
}
