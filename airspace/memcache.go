package airspace

import (
	"bytes"
	"encoding/gob"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/memcache"

	"github.com/skypies/util/ae"
	mcsingleton "github.com/skypies/util/singleton/memcache"
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
// {{{ a.JustAircraftFromMemcacheServer

// This was intended for use from appengine, using a custom dialer from appengine/socket:
//
//   dialer := func(network, addr string, timeout time.Duration) (net.Conn, error) {
//		  return socket.DialTimeout(c, network, addr, timeout)
//	  }
//
// Then we could share a single memcache instance, running on a GCE instance, with
// consolidator. Sadly there is no way for appengine to talk to GCE, without
// exposing the memcached instance to the public internet. So this will all have to wait
// until Google Cloud makes that possible.
func (a *Airspace) JustAircraftFromMemcacheServer(ctx context.Context, dialer func(network, addr string, timeout time.Duration) (net.Conn, error)) error {
	sp := mcsingleton.NewProvider("35.239.5.96:11211")
	if dialer != nil {
		sp.SetDialer(dialer)
	}

	err := sp.ReadSingleton(ctx, "consolidated-airspace", nil, a)

	if err == memcache.ErrCacheMiss {
		return nil
	}

	return err
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
