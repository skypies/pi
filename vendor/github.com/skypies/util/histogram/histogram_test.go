package histogram

// go test github.com/skypies/util/histogram

import(
	"fmt"
	"testing"

	hdr "github.com/codahale/hdrhistogram"
)

func TestHappypath (t *testing.T) {
	h := Histogram{NumBuckets:10, ValMin:0, ValMax:100}
	vals := []int{4,4,4,2}
	
	for _,val := range(vals) {
		h.Add(ScalarVal(val))
	}

	stats,_ := h.Stats()
	if stats.N != len(vals) {
		t.Errorf("Added %d, but found %d\n", len(vals), stats.N)
	}
	expected := 3.5
	if stats.Mean != expected {
		t.Errorf("Expected mean %f, but found %f\n", expected, stats.Mean)
	}
}

func TestHDR2Ascii (t *testing.T) {
	oldHist := Histogram{NumBuckets:10, ValMin:0, ValMax:10}

	h := hdr.New(-1000000,1000000,3)

	vals := []int64{4,4,4,2,5,4,3,7,5,8,1,5,4,2,7,0,0,9,7,9,9,9,9,9,2,3,12}
	for _,val := range vals {
		oldHist.Add(ScalarVal(val))
		if err := h.RecordValue(val); err != nil {
			t.Errorf("RecordValue error: %v\n", err)
		}
	}

	old := oldHist.String()
	new := HDR2ASCII(h, 10, 0, 10)

	fmt.Printf("* old: %s\n* new: %s\n", old, new)
	if old != new {
		t.Errorf("strings differ")
	}
}
