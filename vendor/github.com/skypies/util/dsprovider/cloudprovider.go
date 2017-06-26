package dsprovider

// https://godoc.org/cloud.google.com/go/datastore

import(
	"fmt"
	"golang.org/x/net/context"
	"cloud.google.com/go/datastore"
)

// CloudDSProvider implements the DatastoreProvider interface using the cloud datastore API,
// for use outside of appengine environments.
type CloudDSProvider struct {
	Project string
}

func (p CloudDSProvider)flattenQuery(in *Query) *datastore.Query {
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

func  (p CloudDSProvider)unpackKeyer(in Keyer) *datastore.Key {
	if in == nil { return nil }
	return in.(*datastore.Key)
}
func (p CloudDSProvider)unpackKeyers(in []Keyer) []*datastore.Key {
	out := []*datastore.Key{}
	for _,keyer := range in {
		out = append(out, p.unpackKeyer(keyer))
	}
	return out
}
func (p CloudDSProvider)packKeyers(in []*datastore.Key) []Keyer {
	out := []Keyer{}
	for _,k := range in {
		out = append(out, Keyer(k))
	}
	return out
}

func (p CloudDSProvider)GetAll(ctx context.Context, q *Query, dst interface{}) ([]Keyer, error) {
	dsClient, err := datastore.NewClient(ctx, p.Project)
	dsQuery := p.flattenQuery(q)

	keys,err := dsClient.GetAll(ctx, dsQuery, dst)
	keyers := p.packKeyers(keys)

	if err != nil {
		if _,assertionOk := err.(*datastore.ErrFieldMismatch); assertionOk {
			return keyers, ErrFieldMismatch
		}
		return nil, fmt.Errorf("GetAll{cloud}: %v\nQuery: %s", err, q)
	}
	return keyers,nil
}

func (p CloudDSProvider)Get(ctx context.Context, keyer Keyer, dst interface{}) error {
	dsClient, err := datastore.NewClient(ctx, p.Project)
	if err != nil { return err }

	err = dsClient.Get(ctx, p.unpackKeyer(keyer), dst)
	if err == datastore.ErrNoSuchEntity {
		return ErrNoSuchEntity
	} else if err != nil {
		if _,assertionOk := err.(*datastore.ErrFieldMismatch); assertionOk {
			return ErrFieldMismatch
		}
	}
	return err
}

func (p CloudDSProvider)GetMulti(ctx context.Context, keyers []Keyer, dst interface{}) error {
	dsClient, err := datastore.NewClient(ctx, p.Project)
	if err != nil { return err }
	err = dsClient.GetMulti(ctx, p.unpackKeyers(keyers), dst)

	if err == datastore.ErrNoSuchEntity {
		return ErrNoSuchEntity
	} else if err != nil {
		if _,assertionOk := err.(*datastore.ErrFieldMismatch); assertionOk {
			return ErrFieldMismatch
		}
	}
	return err
}

func (p CloudDSProvider)Put(ctx context.Context, keyer Keyer, src interface{}) (Keyer, error) {
	dsClient, err := datastore.NewClient(ctx, p.Project)
	if err != nil { return nil,err }

	key,error := dsClient.Put(ctx, p.unpackKeyer(keyer), src)
	return Keyer(key), error
}	
func (p CloudDSProvider)PutMulti(ctx context.Context, keyers []Keyer, src interface{}) ([]Keyer, error) {
	dsClient, err := datastore.NewClient(ctx, p.Project)
	if err != nil { return nil,err }

	keys,err := dsClient.PutMulti(ctx, p.unpackKeyers(keyers), src)
	return p.packKeyers(keys), err
}
func (p CloudDSProvider)Delete(ctx context.Context, keyer Keyer) error {
	dsClient, err := datastore.NewClient(ctx, p.Project)
	if err != nil { return err }
	err = dsClient.Delete(ctx, p.unpackKeyer(keyer))
	if err ==	datastore.ErrNoSuchEntity { return ErrNoSuchEntity }
	return err
}	
func (p CloudDSProvider)DeleteMulti(ctx context.Context, keyers []Keyer) error {
	dsClient, err := datastore.NewClient(ctx, p.Project)
	if err != nil { return err }
	err = dsClient.DeleteMulti(ctx, p.unpackKeyers(keyers))
	if err ==	datastore.ErrNoSuchEntity { return ErrNoSuchEntity }
	return err
}

func (p CloudDSProvider)NewIncompleteKey(ctx context.Context, kind string, root Keyer) Keyer {
	key := datastore.IncompleteKey(kind, p.unpackKeyer(root))
	return Keyer(key)
}
func (p CloudDSProvider)NewNameKey(ctx context.Context, kind, name string, root Keyer) Keyer {
	key := datastore.NameKey(kind, name, p.unpackKeyer(root))
	return Keyer(key)
}
func (p CloudDSProvider)NewIDKey(ctx context.Context, kind string, id int64, root Keyer) Keyer {
	key := datastore.IDKey(kind, id, p.unpackKeyer(root))
	return Keyer(key)
}

func (p CloudDSProvider)DecodeKey(encoded string) (Keyer, error) {
	key, err := datastore.DecodeKey(encoded)
	return Keyer(key), err
}
func (p CloudDSProvider)KeyParent(in Keyer) Keyer {
	if parentKey := p.unpackKeyer(in).Parent; parentKey != nil {
		return Keyer(parentKey)
	}
	return nil
}
func (p CloudDSProvider)KeyName(in Keyer) string { return p.unpackKeyer(in).Name }
