package dsprovider

import(
	"fmt"
	"reflect"
	"golang.org/x/net/context"
)

/*

 it := db.NewIterator(ctx, p, q, MyObject{})  // Needs an example result item to get the type

 // Optional; use a bigger page size for batching reads
 it.PageSize = 100

 for it.Iterate(ctx) {
   obj := MyObject{}
   keyer := it.Val(&obj)
   // obj = it.ValAsInterface().(MyObject) // Can use type assertion if you want direct assigment
   fmt.Printf("k=%v, v=%s\n", keyer, obj)
 }
 if it.Err() != nil {
   return it.Err()
 }

 */

// Iterator is a batching iterator that executes the full query up front, to get a list of all
// keys; then it fetches pages of results as it works through the keys. We don't use
// datastore.Iterator, as it times out the result set after 60 seconds, out of abundance of
// caution.
type Iterator struct {
	p            DatastoreProvider
	ty           reflect.Type // the type of the thing the caller wants us to get
	PageSize     int

	keyers     []Keyer        // Keys for the (unfetched remainder of the) full result set

  Slice        interface{}  // The current page of results
	sliceKeys  []Keyer        // ... with their keys

	val          interface{}  // The currently fetched value
	keyer        Keyer        // ... and its key ...
	err          error        // ... or the error we bumped into
}

// {{{ NewIterator

// Snarf down all the keys from the get go.
func NewIterator(ctx context.Context, p DatastoreProvider, q *Query, obj interface{}) *Iterator {
	keyers,err := p.GetAll(ctx, q.KeysOnly(), nil)
	iter := Iterator{
		p: p,
		ty: reflect.TypeOf(obj),
		PageSize: 10,
		keyers: keyers,
		err: err,
	}
	return &iter
}

// }}}
// {{{ Iterate

func (iter *Iterator)Iterate(ctx context.Context) bool {
	if iter.err != nil { return false }
	ok := iter.nextInPage(ctx)
	return (ok && iter.err == nil)
}

// }}}
// {{{ Remaining

// Remaining returns how many items are yet to be processed by the caller
func (iter *Iterator)Remaining() int {
	return iter.currSliceSize() + len(iter.keyers)
}

// }}}
// {{{ Err

func (iter *Iterator)Err() error {
	if iter.err == nil { return nil }
	return fmt.Errorf("dsprovider.iterator: {%v}", iter.err)
}

// Convenience function for things that wrap iterator
func (iter *Iterator)SetErr(err error) { iter.err = err }

// }}}
// {{{ Val

func (iter *Iterator)Val(dst interface{}) Keyer {
	reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(iter.val)) // *dst = iter.val
	return iter.keyer
}

// }}}
// {{{ ValAsInterface

// ValAsInterface returns the val as the base interface; it will need a type assertion back
// into the relevant type.
func (iter *Iterator)ValAsInterface() interface{}{
	return iter.val
}

// }}}

// {{{ slice reflection stuff

func (iter *Iterator)currSliceSize() int {
	if iter.Slice == nil {
		return 0
	}
	return reflect.ValueOf(iter.Slice).Len()
}

func (iter *Iterator)newSlice(size int) {
	newSlcVal := reflect.MakeSlice(reflect.SliceOf(iter.ty), size, size)

	// iter.slice might be nil, so can't do reflect.ValueOf(iter.slice).Set(newSlcVal); go up a level
	iterVal := reflect.ValueOf(iter).Elem() // .Elem to dereference the pointer iter
	iterVal.FieldByName("Slice").Set(newSlcVal)
}

func (iter *Iterator)sliceShift() interface{} {
	slcVal := reflect.ValueOf(iter.Slice)
	ret := slcVal.Index(0).Interface()          // ret := slc[0]

	newSlcVal := slcVal.Slice(1, slcVal.Len())  // newslc := slc[1:]

	iterVal := reflect.ValueOf(iter).Elem()
	iterVal.FieldByName("Slice").Set(newSlcVal) // slc = newslc
	return ret
}

// }}}

// {{{ nextInPage

// Returns true if there is more fetching to be done; false if the client should stop.
func (iter *Iterator)nextInPage(ctx context.Context) (bool) {
	if iter.err != nil { return false }
	if iter.PageSize == 0 { panic("pageslice not fit for purpose") }
	
	// No new vals left in the cache; fetch some new ones
	if iter.Slice == nil || iter.currSliceSize() == 0 {
		if len(iter.keyers) == 0 {
			return false // We're all done !
		}

		var keysForThisBatch []Keyer

		nextSliceSize := iter.PageSize
		if len(iter.keyers) < nextSliceSize {
			// Remaining keys not enough for a full page; grab all of 'em
			nextSliceSize = len(iter.keyers)
		}

		keysForThisBatch  = iter.keyers[0:nextSliceSize]
		iter.keyers       = iter.keyers[nextSliceSize:]

		iter.sliceKeys = keysForThisBatch
		iter.newSlice(nextSliceSize)
		
		// Fetch the objects for the keys in this batch, into the user-provided slice.
		// GetAll(ctx context.Context, q *Query, dst interface{}) ([]Keyer, error)
		// if err := datastore.GetMulti(iter.Ctx, keysForThisBatch, iter.PageSlice); err != nil {
		if err := iter.p.GetMulti(ctx, keysForThisBatch, iter.Slice); err != nil {
			iter.err = err
			return false
		}
	}

	// We should have unreturned results in the cache, one way or another; shift & return the first
	iter.val = iter.sliceShift()
	iter.keyer = iter.sliceKeys[0]
	iter.sliceKeys = iter.sliceKeys[1:]
	
	return true
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
