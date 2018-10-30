package airspace

import (
	"bytes"
	"encoding/gob"
	
	"golang.org/x/net/context"
	"google.golang.org/appengine/memcache"

	"github.com/skypies/util/ae"
)

// {{{ a.ToBytes

func (a *Airspace)ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(a); err != nil {
		return nil,err
	}
	ret := buf.Bytes()
	return ret, nil
}

// }}}
// {{{ a.FromBytes

func (a *Airspace)FromBytes(b []byte) error {
	buf := bytes.NewBuffer(b)
	return gob.NewDecoder(buf).Decode(a)
}

// }}}

// Two different storers, into two different memcache entities (one small, one sharded)
// {{{ a.JustAircraftToMemcache

func (a *Airspace)JustAircraftToMemcache(c context.Context) error {
	aJustAircraft := Airspace{Aircraft: a.Aircraft}

	b,err := aJustAircraft.ToBytes()
	if err != nil { return err }

	return ae.SaveSingletonToMemcache(c, "airspace", b)
}

// }}}
// {{{ a.JustAircraftFromMemcache

func (a *Airspace) JustAircraftFromMemcache(c context.Context) error {
	if b,err := ae.LoadSingletonFromMemcache(c, "airspace"); err == nil {
		if err := a.FromBytes(b); err != nil {
			return err
		}
	} else if err != memcache.ErrCacheMiss {
		return err
	}

	return nil
}

// }}}

// {{{ a.EverythingToMemcache

func (a *Airspace)EverythingToMemcache(c context.Context) error {
	b,err := a.ToBytes()
	if err != nil { return err }

	return ae.SaveShardedSingletonToMemcache(c, "deduping-signatures", b)
}

// }}}
// {{{ a.EverythingFromMemcache

func (a *Airspace)EverythingFromMemcache(c context.Context) error {
	if b,err := ae.LoadShardedSingletonFromMemcache(c, "deduping-signatures"); err == nil {
		if err := a.FromBytes(b); err != nil {
			return err
		}
	} else if err != memcache.ErrCacheMiss {
		return err
	}

	return nil
}

// }}}


// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
