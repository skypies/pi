package airspace

import (
	"bytes"
	"encoding/gob"
	"fmt"
	
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
)

const chunksize = 950000

// {{{ bytesToMemcacheShards

// Object usually too big (1MB limit), so shard.
// http://stackoverflow.com/questions/9127982/
func bytesToMemcacheShards(key string, b []byte) {
	c := appengine.BackgroundContext()

	items := []*memcache.Item{}
	for i:=0; i<len(b); i+=chunksize {
		k := fmt.Sprintf("=%d=%s",i,key)
		s,e := i, i+chunksize-1
		if e>=len(b) { e = len(b)-1 }
		items = append(items, &memcache.Item{ Key:k , Value:b[s:e+1] }) // slice sytax is [s,e)
	}

	if err := memcache.SetMulti(c, items); err != nil {
		log.Errorf(c, " #=== cdb sharded store fail: %v", err)
	}
}

// }}}
// {{{ bytesFromMemcacheShards

// bool means 'found'
func bytesFromMemcacheShards(key string) ([]byte, bool) {
	c := appengine.BackgroundContext()

	keys := []string{}
	for i:=0; i<32; i++ { keys = append(keys, fmt.Sprintf("=%d=%s",i*chunksize,key)) }

	if items,err := memcache.GetMulti(c, keys); err != nil {
		log.Errorf(c, "fdb memcache multiget: %v", err)
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

func (a *Airspace)ToMemcache() error {
	b,err := a.ToBytes()
	if err != nil { return err }

	bytesToMemcacheShards("airspace", b)
	return nil
}

// }}}
// {{{ a.FromMemcache

func (a *Airspace) FromMemcache() error {
	if b,found := bytesFromMemcacheShards("airspace"); found==true {
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
