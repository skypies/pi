package dsprovider

import(
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest" // Also used for testing Cloud API, in theory
)

const AppID = "mytestapp"
const TestKind = "test"
type Testobj struct {
	I,J int
	S   string
}

// {{{ newConsistentContext

// A version of aetest.NewContext() that has a consistent datastore - so we can read our writes.
func newConsistentContext() (context.Context, func(), error) {
	inst, err := aetest.NewInstance(&aetest.Options{
		StronglyConsistentDatastore: true,
		AppID: AppID,
	})
	if err != nil {
		return nil, nil, err
	}
	req, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		inst.Close()
		return nil, nil, err
	}
	ctx := appengine.NewContext(req)
	return ctx, func() {
		inst.Close()
	}, nil
}

// }}}
// {{{ putObjs

func putObjs(ctx context.Context, p DatastoreProvider, t *testing.T, n int) ([]Testobj, []Keyer) {
	objs := []Testobj{}
	keyers := []Keyer{}
	for i:=0; i<n; i++ {
		obj := Testobj{I: i*3}
		keyer := p.NewNameKey(ctx, TestKind, fmt.Sprintf("name%d", i), nil)
		fullKeyer,err := p.Put(ctx, keyer, &obj)
		if err != nil {
			t.Errorf("Put on %#v failed with err: %v\n", obj, err)
		}
		objs = append(objs, obj)
		keyers = append(keyers, fullKeyer)
	}

	return objs,keyers
}

// }}}

func TestProviderAPI(t *testing.T) {
	testProviderAPI(t, AppengineDSProvider{})
	// Sadly, the aetest framework hangs on the first Put from the cloud client
	//testProviderAPI(t, CloudDSProvider{AppID})
}
// {{{ testProviderAPI

func testProviderAPI(t *testing.T, p DatastoreProvider) {
	ctx, done, err := newConsistentContext()
	if err != nil { t.Fatal(err) }
	defer done()

	// query runner
	runQ := func(expected int, q *Query) []Testobj {
		results := []Testobj{}
		if _,err := p.GetAll(ctx, q, &results); err != nil {
			t.Fatal(err)
		} else if len(results) != expected {
			t.Errorf("expected %d results, saw %d; query: %s", expected, len(results), q)
			for i,f := range results { fmt.Printf("result [%3d] %s\n", i, f) }
		}
		return results
	}

	// Insert a few things
	objs,keyers := putObjs(ctx, p, t, 3)

	if parent := p.KeyParent(keyers[0]); parent != nil {
		t.Errorf("KeyParent lookup wasn't nil: %#v", parent)
	}
	if name := p.KeyName(keyers[0]); name != "name0" {
		t.Errorf("KeyName looked up %q, not \"name0\"\n", name)
	}

	// Lookup a few things
	runQ(len(objs), NewQuery(TestKind))
	runQ(2,         NewQuery(TestKind).Limit(2))
	runQ(1,         NewQuery(TestKind).Filter("I = ", 6))
	proj := runQ(1, NewQuery(TestKind).Filter("I = ", 6).Project("J"))
	if proj[0].I != 0 {
		t.Errorf("We saw an I value when we only projected J\n")
	}
	
	// Now delete something, and see it vanish
	if err := p.Delete(ctx, keyers[0]); err != nil {
		t.Errorf("p.Delete failed: %v\n", err)
	}
	
	runQ(len(objs)-1, NewQuery(TestKind))

	result := Testobj{}
	if err := p.Get(ctx, keyers[0], &result); err != ErrNoSuchEntity {
		t.Errorf("Failed to recast ErrNoSuchEntity")
	}
	
	keyers = keyers[1:]
	
	results := make([]Testobj, len(keyers)) // dst must be same length as keyers for GetMulti
	if err := p.GetMulti(ctx,keyers,results); err != nil {
		t.Errorf("p.GetMulti failed: %v\n", err)
	}
	if len(results) != 2 {
		t.Errorf("p.GetMulti: expected %d, saw %d\n", 2, len(results))
	}

	// PutMulti
	multiObjs := []Testobj{}
	multiKeyers := []Keyer{}
	for i:=0; i<452; i++ {
		obj := Testobj{I: (100+i)*3}
		keyer := p.NewNameKey(ctx, TestKind, fmt.Sprintf("name%d", i+100), nil)
		multiKeyers = append(multiKeyers, keyer)
		multiObjs = append(multiObjs, obj)
	}

	if _,err := p.PutMulti(ctx, multiKeyers, multiObjs); err != nil {
		t.Errorf("PutMulti on %d objs failed with err: %v\n", len(multiObjs), err)
	}
}

// }}}

func TestIterator(t *testing.T) {
	testIterator(t, AppengineDSProvider{})
}
// {{{ testIterator

func testIterator(t *testing.T, p DatastoreProvider) {
	ctx, done, err := newConsistentContext()
	if err != nil { t.Fatal(err) }
	defer done()

	// Insert a few things
	nObj := 11
	_,_ = putObjs(ctx, p, t, nObj)

	it := NewIterator(ctx, p, NewQuery(TestKind).Order("I"), Testobj{})  // Needs an example item

	it.PageSize = 3 // use a page size that isn't a factor of the size of the result set
	
	n := 0

	obj1,obj2 := Testobj{},Testobj{}
	for it.Iterate(ctx) {
		keyer := it.Val(&obj1)
		obj2 = it.ValAsInterface().(Testobj)
		fmt.Printf("%v: %#v, %#v\n", keyer, obj1, obj2)
		n++
	}
	if it.Err() != nil {
		t.Errorf("test iterator err: %v\n", it.Err())
	}

	if n != nObj {
		t.Errorf("test expected to see %d, but saw %d\n", nObj, n)
	}
}

// }}}


// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
