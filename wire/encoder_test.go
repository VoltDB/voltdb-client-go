package wire

import (
	"bytes"
	"testing"
	"time"
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

func TestEncoder_Int32(t *testing.T) {
	t.Parallel()
	sample := []int32{-100, -1, 0, 1, 100}
	e := NewEncoder()
	for _, v := range sample {
		e.Reset()
		n, err := e.Int32(v)
		if err != nil {
			t.Fatal(err)
		}
		if n != integerSize {
			t.Errorf("expected %d got %d", integerSize, n)
		}
	}
}

func TestEncoder_Float64(t *testing.T) {
	t.Parallel()

	sample := []float64{-100.1, -1.01, 0.0, 1.01, 100.1}
	e := NewEncoder()

	for _, v := range sample {
		n, err := e.Float64(v)
		if err != nil {
			t.Error(err)
		}
		if n != longSize {
			t.Errorf("expected %d got %d", longSize, n)
		}
	}
}

func TestEncoder_String(t *testing.T) {
	t.Parallel()
	expected := []byte{0x00, 0x00, 0x00, 0x06, 'a', 'b', 'c', 'd', 'e', 'f'}
	s := "abcdef"

	e := NewEncoder()

	n, err := e.String(s)
	if err != nil {
		t.Fatal(err)
	}
	ns := len(s) + integerSize
	if n != ns {
		t.Errorf("expected %d got %d", ns, n)
	}

	b := e.Bytes()
	if !bytes.Equal(b, expected) {
		t.Errorf("expected %s got %s", string(expected), string(b))
	}
}

func TestEncoder_Time(t *testing.T) {
	t.Parallel()
	e := NewEncoder()

	n, err := e.Time(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if n != longSize {
		t.Errorf("expected %d got %d", longSize, n)
	}

	//TODO(gernest): Add Decoder to verify the encoded values for time are
	//correct.
}

func TestEncoder_PtrParam(t *testing.T) {
	array := []int32{11, 12, 13}
	e := NewEncoder()
	_, err := e.Marshal(&array)
	if err != nil {
		t.Fatal(err)
	}
	expLen := 18
	if e.Len() != expLen {
		t.Fatalf("expected %d got %d", expLen, e.Len())
	}
}

func TestEncoder_StringSliceParam(t *testing.T) {
	array := []string{"zero", "one", "two", "three", "four", "five", "six",
		"seven", "eight", "nine", "ten", "eleven",
		"twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
		"eighteen", "nineteen"}
	expLen := 213
	e := NewEncoder()
	_, err := e.Marshal(array)
	if err != nil {
		t.Fatal(err)
	}
	if e.Len() != expLen {
		t.Fatalf("expected %d got %d", expLen, e.Len())
	}
}

func TestEncoder_FloatSLiceParam(t *testing.T) {
	array := []float64{-459.67, 32.0, 212.0}
	expLen := 30
	e := NewEncoder()
	_, err := e.Marshal(array)
	if err != nil {
		t.Fatal(err)
	}

	if e.Len() != expLen {
		t.Fatalf("expected %d got %d", e.Len(), expLen)
	}
}
