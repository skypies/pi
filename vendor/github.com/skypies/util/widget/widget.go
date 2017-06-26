// This file contains a few routines for parsing form values
package widget

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"github.com/skypies/util/date"
)

// {{{ FormValueDateRange

// This widget assumes the values 'date', 'range_from', and 'range_to'
func FormValueDateRange(r *http.Request) (s,e time.Time, err error) {
	err = nil

	switch r.FormValue("date") {
	case "today":
		s,_ = date.WindowForToday()
		e=s
	case "yesterday":
		s,_ = date.WindowForYesterday()
		e=s
	case "day":
		s = date.ArbitraryDatestring2MidnightPdt(r.FormValue("day"), "2006/01/02")
		e=s
	case "range":
		s = date.ArbitraryDatestring2MidnightPdt(r.FormValue("range_from"), "2006/01/02")
		e = date.ArbitraryDatestring2MidnightPdt(r.FormValue("range_to"), "2006/01/02")
		if s.After(e) { s,e = e,s }
	}
	
	e = e.Add(23*time.Hour + 59*time.Minute + 59*time.Second) // make sure e covers its whole day

	return
}

// }}}
// {{{ FormValue{Int64,IntWithDefault}

func FormValueInt64(r *http.Request, name string) int64 {
	val,_ := strconv.ParseInt(r.FormValue(name), 10, 64)
	return val
}

func FormValueIntWithDefault(r *http.Request, name string, dflt int) int {
	if val := FormValueInt64(r, name); val != 0 {
		return int(val)
	}
	return dflt
}

// }}}
// {{{ FormValueDuration

// As well as the normal syntax, we support "7d", where a day is 24*time.Hour.
func FormValueDuration(r *http.Request, name string) time.Duration {
	val,err := time.ParseDuration(r.FormValue(name))
	if err != nil {
		// Check for 'd', our own pseudo-duration
		bits := regexp.MustCompile("^([0-9])+d$").FindStringSubmatch(r.FormValue(name))
		if bits != nil && len(bits)==2 {
			num,_ := strconv.ParseInt(bits[1], 10, 64)
			val = time.Hour * 24 * time.Duration(num)
		}
	}

	return val
}

// }}}
// {{{ FormValueEpochTime

func FormValueEpochTime(r *http.Request, name string) time.Time {
	return time.Unix(FormValueInt64(r,name), 0)
}

// }}}
// {{{ FormValueFloat64{,WithDefault}

func FormValueFloat64(w http.ResponseWriter, r *http.Request, name string) float64 {	
	if val,err := strconv.ParseFloat(r.FormValue(name), 64); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return -1
	} else {
		return val
	}
}

func FormValueFloat64WithDefault(r *http.Request, name string, dflt float64) float64 {
	if val,err := strconv.ParseFloat(r.FormValue(name), 64); err == nil {
		return val
	}
	return dflt
}

// }}}
// {{{ FormValueFloat64EatErrs

func FormValueFloat64EatErrs(r *http.Request, name string) float64 {	
	if val,err := strconv.ParseFloat(r.FormValue(name), 64); err != nil {
		return 0.0
	} else {
		return val
	}
}

// }}}
// {{{ FormValueCheckbox

func FormValueCheckbox(r *http.Request, name string) bool {
	if r.FormValue(name) != "" {
		return true
	}
	return false
}

// }}}
// {{{ FormValueCommaSepStrings

func FormValueCommaSepStrings(r *http.Request, name string) []string {
	ret := []string{}
	r.ParseForm()
	for _,v := range r.Form[name] {
		for _,str := range strings.Split(v, ",") {
			if str != "" {
				ret = append(ret, str)
			}
		}
	}
	return ret
}

// }}}
// {{{ FormValueCommaSpaceSepStrings

// Separate by comma, or space
func FormValueCommaSpaceSepStrings(r *http.Request, name string) []string {
	ret := []string{}
	for _,v := range FormValueCommaSepStrings(r,name) {
		for _,str := range strings.Split(v, " ") {
			if str != "" {
				ret = append(ret, str)
			}
		}
	}
	return ret
}

// }}}

// {{{ DateRangeToValues

func DateRangeToValues(s,e time.Time) url.Values {
	v := url.Values{}
	v.Add("date", "range")
	v.Add("range_from", s.Format("2006/01/02"))
	v.Add("range_to", e.Format("2006/01/02"))
	return v
}

// }}}
// {{{ DateRangeToCGIArgs

func DateRangeToCGIArgs(s,e time.Time) string {
	return DateRangeToValues(s,e).Encode()
}

// }}}
// {{{ ExtractAllCGIArgs, URLStringReplacingGETArgs

// ExtractAllCGIArgs gets both GET and POST arguments into a single map
func ExtractAllCGIArgs(r *http.Request) url.Values {
	r.ParseForm()
	vals := r.URL.Query()
	for k,formvals := range r.Form {
		for _,formval := range formvals {
			vals.Set(k,formval)
		}
	}
	return vals
}

// Creates a URL that matches the request, adding the vals as CGI GET parameters
func URLStringReplacingGETArgs(r *http.Request, vals *url.Values) string {
	urlstr := fmt.Sprintf("%s://%s%s", r.URL.Scheme, r.Host, r.URL.Path)

	if vals != nil {
		urlstr += fmt.Sprintf("?%s", vals.Encode())
	}

	return urlstr
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
