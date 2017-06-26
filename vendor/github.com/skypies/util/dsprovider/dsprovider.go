package dsprovider

import(
	"errors"
	"golang.org/x/net/context"
)

var(
	ErrNoSuchEntity = errors.New("dsprovider: no such entity")
	ErrFieldMismatch = errors.New("dsprovider: src obj had a field that dst obj didn't")
)

// Keyer is a very thin wrapper. It should be populated with a *datastore.Key
type Keyer interface {
	Encode() string
}

// DatastoreProvider is a wrapper over the datastore APIs (cloud and appengine), so that
// client code (and in particular query assembley) can run both inside and outside of google
// appengine.
type DatastoreProvider interface {
	Get(ctx context.Context, keyer Keyer, dst interface{}) error
	GetMulti(ctx context.Context, keyers []Keyer, dst interface{}) error
	GetAll(ctx context.Context, q *Query, dst interface{}) ([]Keyer, error)
	Put(ctx context.Context, keyer Keyer, src interface{}) (Keyer, error)
	PutMulti(ctx context.Context, keyers []Keyer, src interface{}) ([]Keyer, error)
	Delete(ctx context.Context, keyer Keyer) error
	DeleteMulti(ctx context.Context, keyers []Keyer) error
	
	NewIncompleteKey(ctx context.Context, kind string, root Keyer) Keyer
	NewNameKey(ctx context.Context, kind, name string, root Keyer) Keyer
	NewIDKey(ctx context.Context, kind string, id int64, root Keyer) Keyer

	DecodeKey(encoded string) (Keyer, error)
	KeyParent(Keyer) Keyer
	KeyName(Keyer) string

	// Infof, etc ?
}

