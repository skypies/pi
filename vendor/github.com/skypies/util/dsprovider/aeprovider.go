package dsprovider

// https://cloud.google.com/appengine/docs/standard/go/datastore/reference

import(
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

// AppengineDSProvider implements the DatastoreProvider interface using the appengine datastore API,
// for use inside appengine environments.
type AppengineDSProvider struct {
}

func (p AppengineDSProvider)FlattenQuery(in *Query) *datastore.Query {
	out := datastore.NewQuery(in.Kind)
	if in.AncestorKeyer != nil { out = out.Ancestor(in.AncestorKeyer.(*datastore.Key)) }
	for _,filter := range in.Filters {
		out = out.Filter(filter.Field, filter.Value)
	}
	if len(in.ProjectFields) != 0 { out = out.Project(in.ProjectFields...) }
	if in.OrderStr != ""          { out = out.Order(in.OrderStr) }
	if in.KeysOnlyVal             { out = out.KeysOnly() }
	if in.LimitVal != 0           { out = out.Limit(in.LimitVal) }
	return out
}

func (p AppengineDSProvider)unpackKeyer(in Keyer) *datastore.Key {
	if in == nil { return nil }
	return in.(*datastore.Key)
}
func (p AppengineDSProvider)unpackKeyers(in []Keyer) []*datastore.Key {
	out := []*datastore.Key{}
	for _,keyer := range in {
		out = append(out, p.unpackKeyer(keyer))
	}
	return out
}
func (p AppengineDSProvider)packKeyers(in []*datastore.Key) []Keyer {
	out := []Keyer{}
	for _,k := range in {
		out = append(out, Keyer(k))
	}
	return out
}


func (p AppengineDSProvider)GetAll(ctx context.Context, q *Query, dst interface{}) ([]Keyer, error) {
	aeQuery := p.FlattenQuery(q)	
	keys,err := aeQuery.GetAll(ctx, dst)
	keyers := p.packKeyers(keys)

	if err != nil {
		if _,assertionOk := err.(*datastore.ErrFieldMismatch); assertionOk {
			return keyers, ErrFieldMismatch
		}
		return nil, fmt.Errorf("GetAll{AE}: %v\nQuery: %s", err, q)
	}

	return keyers,nil
}

func (p AppengineDSProvider)Get(ctx context.Context, keyer Keyer, dst interface{}) error {
	err := datastore.Get(ctx, p.unpackKeyer(keyer), dst)

	if err == datastore.ErrNoSuchEntity {
		return ErrNoSuchEntity
	} else if err != nil {
		if _,assertionOk := err.(*datastore.ErrFieldMismatch); assertionOk {
			return ErrFieldMismatch
		}
	}
	return err
}

func (p AppengineDSProvider)GetMulti(ctx context.Context, keyers []Keyer, dst interface{}) error {
	err := datastore.GetMulti(ctx, p.unpackKeyers(keyers), dst)

	if err == datastore.ErrNoSuchEntity {
		return ErrNoSuchEntity
	} else if err != nil {
		if _,assertionOk := err.(*datastore.ErrFieldMismatch); assertionOk {
			return ErrFieldMismatch
		}
	}
	return err
}

func (p AppengineDSProvider)Put(ctx context.Context, keyer Keyer, src interface{}) (Keyer, error) {
	key,error := datastore.Put(ctx, p.unpackKeyer(keyer), src)
	return Keyer(key), error
}	
func (p AppengineDSProvider)PutMulti(ctx context.Context, keyers []Keyer, src interface{}) ([]Keyer, error) {
	keys,err := datastore.PutMulti(ctx, p.unpackKeyers(keyers), src)
	return p.packKeyers(keys), err
}
func (p AppengineDSProvider)Delete(ctx context.Context, keyer Keyer) error {
	err := datastore.Delete(ctx, p.unpackKeyer(keyer))
	if err ==	datastore.ErrNoSuchEntity { return ErrNoSuchEntity }
	return err
}	
func (p AppengineDSProvider)DeleteMulti(ctx context.Context, keyers []Keyer) error {
	err := datastore.DeleteMulti(ctx, p.unpackKeyers(keyers))
	if err ==	datastore.ErrNoSuchEntity { return ErrNoSuchEntity }
	return err
}	

func (p AppengineDSProvider)NewIncompleteKey(ctx context.Context, kind string, root Keyer) Keyer {
	key := datastore.NewIncompleteKey(ctx, kind, p.unpackKeyer(root))
	return Keyer(key)
}
func (p AppengineDSProvider)NewNameKey(ctx context.Context, kind, name string, root Keyer) Keyer {
	key := datastore.NewKey(ctx, kind, name, 0, p.unpackKeyer(root))
	return Keyer(key)
}
func (p AppengineDSProvider)NewIDKey(ctx context.Context, kind string, id int64, root Keyer) Keyer {
	key := datastore.NewKey(ctx, kind, "", id, p.unpackKeyer(root))
	return Keyer(key)
}

func (p AppengineDSProvider)DecodeKey(encoded string) (Keyer, error) {
	key, err := datastore.DecodeKey(encoded)
	return Keyer(key), err
}
func (p AppengineDSProvider)KeyParent(in Keyer) Keyer {
	if parentKey := p.unpackKeyer(in).Parent(); parentKey != nil {
		return Keyer(parentKey)
	}
	return nil
}
func (p AppengineDSProvider)KeyName(in Keyer) string { return p.unpackKeyer(in).StringID() }
