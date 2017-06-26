package gaeutil

import(
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

/*

type Foo struct {
  S string
}

foo := Foo{S:"hello"}
err1 := gaeutil.SaveAnySingleton(ctx, "Foo_007", &foo)

foo2 := Foo{}
err2 := gaeutil.LoadAnySingleton(ctx, "Foo_007", &foo2)

 */

func singletonDSKey(c context.Context, name string) *datastore.Key {
	return datastore.NewKey(c, "Singleton", name, 0, nil)
}

type Singleton struct {
	Value []byte `datastore:",noindex"`
}

var ErrNoSuchEntityDS = errors.New("gaeutil/datastore: no such entity")

func LoadSingletonFromDatastore(c context.Context, name string) ([]byte, error) {
	s := Singleton{}
	if err := datastore.Get(c, singletonDSKey(c,name), &s); err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil,ErrNoSuchEntityDS // Wrap this, so clients don't need to import datastore
		}
		return nil,err
	}
	return s.Value,nil
}

func SaveSingletonToDatastore(c context.Context, name string, data []byte) error {
	if len(data) > 950000 {
		return fmt.Errorf("singleton too large (name=%s, size=%d)", name, len(data))
	}
	s := Singleton{data}
	_,err := datastore.Put(c, singletonDSKey(c,name), &s)
	return err
}

func LoadSingleton(c context.Context, name string) ([]byte, error) {
	data,err := LoadSingletonFromMemcache(c,name)
	if err != nil {
		// We don't care if it was a cache miss or a deeper error - failback to datastore either way
		data,err = LoadSingletonFromDatastore(c,name)

		// Why swallow this error ?
		if err == datastore.ErrNoSuchEntity {
			return nil,nil
		}
	}
	return data,nil
}

func SaveSingleton(c context.Context, name string, data []byte) error {
	if err := SaveSingletonToDatastore(c,name,data); err != nil {
		return err
	}

	SaveSingletonToMemcache(c,name,data)  // Don't care if this fails
	return nil
}

func DeleteSingleton(ctx context.Context, name string) error {
	DeleteSingletonFromMemcache(ctx, name) // Don't care about memcache.ErrCacheMiss
	return datastore.Delete(ctx, singletonDSKey(ctx,name))
}

func LoadAnySingleton(ctx context.Context, name string, obj interface{}) error {
	myBytes,err := LoadSingleton(ctx, name)

	if err == ErrNoSuchEntityDS {
		// Debug codepath; LoadSingleton swallows this, but if we use *FromDatastore we see it
		return nil
	} else if err != nil {
		return err
	} else if myBytes == nil {
		// This happens if the object was not found; don't try to decode it.
		return nil
	}

	buf := bytes.NewBuffer(myBytes)

	if err := gob.NewDecoder(buf).Decode(obj); err != nil {
		return err
	}

	return nil
}

func SaveAnySingleton(ctx context.Context, name string, obj interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(obj); err != nil {
		return err
	}
	
	return SaveSingleton(ctx, name, buf.Bytes())
}
