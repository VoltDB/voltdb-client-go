package voltdbclient

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"reflect"
	"sort"
)

// Int64Slice attaches the methods of sort.Interface to []int64, sorting in increasing order.
type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort is a convenience method.
func (s Int64Slice) Sort() {
	sort.Sort(s)
}

// SearchInt64s searches for x in a sorted slice of int64 and returns the index
// as specified by sort.Search. The slice must be sorted in ascending order.
func SearchInt64s(a []int64, x int64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

// helper function for clearing content of any type
func clear(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

func fromGzip(compressed []byte) ([]byte, error) {
	var b = bytes.NewReader(compressed)
	r, err := gzip.NewReader(b)
	defer r.Close()
	if err != nil {
		return nil, err
	}
	decompressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return decompressed, nil
}
