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
	if n != byteSize {
		t.Errorf("expected %d got %d", byteSize, n)
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

func TestEncoder_Int16(t *testing.T) {
	t.Parallel()

	var s1 int16 = 0x4BCD

	e := NewEncoder()
	n, err := e.Int16(s1)
	if err != nil {
		t.Fatal(err)
	}
	if n != shortSize {
		t.Errorf("expected %d got %d", shortSize, n)
	}

}
