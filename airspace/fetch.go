package airspace

import(
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/skypies/geo"
)

// For clients, fetching from a pi/frontend via JSON
func Fetch(client *http.Client, host string, src string, bbox geo.LatlongBox) (*Airspace, error) {
	as := Airspace{}
	if host == "" { host = "fdb.serfr1.org" }
	if src == "" { src = "fdb" }

	url := fmt.Sprintf("http://%s/?json=1&src=%s&%s", host, src, bbox.ToCGIArgs("box"))

	if resp,err := client.Get(url); err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf ("Bad status: %v", resp.Status)
	} else if err := json.NewDecoder(resp.Body).Decode(&as); err != nil {
		return nil, err
	}

	return &as, nil
}
