package voltdb

import (
	"testing"
)

func TestEncoder_Byte(t *testing.T) {
	t.Parallel()

	var s byte = 0x10

	e := NewEncoder()

	n, err := e.Byte(int8(s))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 got %d", n)
	}
	b := e.Bytes()
	if b[0] != s {
		t.Error("expected the same byte")
	}

	sample := []int8{-128, -10, 0, 127}
	for _, val := range sample {
		e.Reset()
		_, err = e.Byte(val)
		if err != nil {
			t.Error(err)
		}

		//TODO(gernest): Add Decoding implementation to verify that the encoded
		//values are correct.
	}
}
