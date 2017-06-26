package date

import(
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

func formValueInt64EatErrs(r *http.Request, name string) int64 {
	val,_ := strconv.ParseInt(r.FormValue(name), 10, 64)
	return val
}

// If fields absent or blank, returns {0.0, 0.0}
func FormValueTimeOfDayRange(r *http.Request, stem string) (TimeOfDayRange,error) {
	return ParseTimeOfDay(r.FormValue(stem+"_start"), r.FormValue(stem+"_len"),)
}

func (tr TimeOfDayRange)Values() url.Values {
	v := url.Values{}
	v.Add("start", fmt.Sprintf("%02d:%02d", tr.Hour, tr.Minute))
	v.Add("len", tr.Length.String())
	return v
}
