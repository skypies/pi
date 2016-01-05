package airspace

import (
	"bytes"
	"encoding/gob"
	"fmt"
	
	"golang.org/x/net/context"

	"google.golang.org/appengine/memcache"
)

const chunksize = 950000

// {{{ bytesToMemcacheShards

// Object usually too big (1MB limit), so shard.
// http://stackoverflow.com/questions/9127982/
func bytesToMemcacheShards(c context.Context, key string, b []byte) error {
	// c := appengine.BackgroundContext()
	items := []*memcache.Item{}
	for i:=0; i<len(b); i+=chunksize {
		k := fmt.Sprintf("=%d=%s",i,key)
		s,e := i, i+chunksize-1
		if e>=len(b) { e = len(b)-1 }
		items = append(items, &memcache.Item{ Key:k , Value:b[s:e+1] }) // slice sytax is [s,e)
	}

	return memcache.SetMulti(c, items)
}

// }}}
// {{{ bytesFromMemcacheShards

// bool means 'found'
func bytesFromMemcacheShards(c context.Context, key string) ([]byte, bool) {
	keys := []string{}
	for i:=0; i<32; i++ { keys = append(keys, fmt.Sprintf("=%d=%s",i*chunksize,key)) }

	if items,err := memcache.GetMulti(c, keys); err != nil {
		fmt.Printf("bytesFromMemcacheShards/GetMulti err: %v\n", err)
		return nil,false

	} else {
		b := []byte{}
		for i:=0; i<32; i++ {
			if item,exists := items[keys[i]]; exists==false {
				break
			} else {
				b = append(b, item.Value...)
			}
		}

		if len(b) > 0 {
			return b, true
		} else {
			return nil, false
		}
	}
}

// }}}

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

// {{{ a.ToMemcache

func (a *Airspace)ToMemcache(c context.Context) error {
	b,err := a.ToBytes()
	if err != nil { return err }

	return bytesToMemcacheShards(c, "airspace", b)
}

// }}}
// {{{ a.FromMemcache

func (a *Airspace) FromMemcache(c context.Context) error {
	if b,found := bytesFromMemcacheShards(c, "airspace"); found==true {
		if err := a.FromBytes(b); err != nil {
			return err
		}
	}
	return nil
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
