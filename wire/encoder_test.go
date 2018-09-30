package wire

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"
)

func TestEncoder_MarshalNil(t *testing.T) {
	t.Parallel()
	e := NewEncoder()
	n, err := e.MarshalNil()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected %d got %d", ByteSize, n)
	}

	b := e.Bytes()
	if b[0] != byte(NullColumn) {
		t.Errorf("expected NullColumn byte %d got %d", NullColumn, b[0])
	}
}

func TestEncoder_Nil(t *testing.T) {
	testCases := []struct {
		val  interface{}
		size int
	}{
		{
			val:  (*string)(nil),
			size: 1,
		},
		{
			val:  nil,
			size: 1,
		},
	}

	for i := range testCases {
		e := NewEncoder()
		n, err := e.Marshal(testCases[i].val)
		if err != nil {
			t.Fatal(err)
		}
		if n != testCases[i].size {
			t.Errorf("expected size %d got %d", testCases[i].size, n)
		}
	}
}

func TestEncoder_Byte(t *testing.T) {
	t.Parallel()

	var s byte = 0x10

	e := NewEncoder()

	n, err := e.Byte(int8(s))
	if err != nil {
		t.Fatal(err)
	}
	if n != ByteSize {
		t.Errorf("expected %d got %d", ByteSize, n)
	}
	b := e.Bytes()
	if b[0] != s {
		t.Error("expected the same byte")
	}

	sample := []int8{-128, -10, 0, 127}
	d := &Decoder{}
	for _, val := range sample {
		e.Reset()
		_, err = e.Byte(val)
		if err != nil {
			t.Error(err)
		}
		d.SetReader(e)
		b, err := d.Byte()
		if err != nil {
			t.Fatal(err)
		}
		if val != b {
			t.Errorf("expected %d got %d", val, b)
		}
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
	if n != ShortSize {
		t.Errorf("expected %d got %d", ShortSize, n)
	}

	d := NewDecoder(e)
	b1, err := d.Byte()
	if err != nil {
		t.Fatal(err)
	}

	b2, err := d.Byte()
	if err != nil {
		t.Fatal(err)
	}
	if byte(b1) != 0x4B && byte(b2) != 0xCD {
		t.Errorf("writeShort has %v,%v wants %v,%v", b1, b2, 0xCD, 0x4B)
	}
}

func TestEncoder_Int32(t *testing.T) {
	t.Parallel()
	sample := []int32{-100, -1, 0, 1, 100}
	e := NewEncoder()
	d := &Decoder{}
	for _, v := range sample {
		e.Reset()
		n, err := e.Int32(v)
		if err != nil {
			t.Fatal(err)
		}
		if n != IntegerSize {
			t.Errorf("expected %d got %d", IntegerSize, n)
		}
		d.SetReader(e)
		i, err := d.Int32()
		if err != nil {
			t.Fatal(err)
		}
		if v != i {
			t.Errorf("expected %v have %v", v, i)
		}
	}
}

func TestEncoder_Float64(t *testing.T) {
	t.Parallel()

	sample := []float64{-100.1, -1.01, 0.0, 1.01, 100.1}
	e := NewEncoder()
	d := &Decoder{}
	for _, v := range sample {
		e.Reset()
		n, err := e.Float64(v)
		if err != nil {
			t.Error(err)
		}
		if n != LongSize {
			t.Errorf("expected %d got %d", LongSize, n)
		}
		d.SetReader(e)
		f, err := d.Float64()
		if err != nil {
			t.Fatal(err)
		}
		if v != f {
			t.Errorf("expected %v have %v", v, f)
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
	ns := len(s) + IntegerSize
	if n != ns {
		t.Errorf("expected %d got %d", ns, n)
	}

	b := e.Bytes()
	if !bytes.Equal(b, expected) {
		t.Errorf("expected %s got %s", string(expected), string(b))
	}
	d := NewDecoder(e)
	for idx, val := range expected {
		actual, err := d.Byte()
		if err != nil {
			t.Fatal(err)
		}
		if val != byte(actual) {
			t.Errorf("writeString at index %v has %v wants %v", idx, actual, val)
		}
	}

	val := "⋒♈ℱ8 ♈ᗴᔕ♈ ᔕ♈ᖇᓰﬡᘐ"
	e.Reset()
	_, err = e.String(val)
	if err != nil {
		t.Fatal(err)
	}
	d.SetReader(e)
	result, err := d.String()
	if err != nil {
		t.Fatal(err)
	}
	if val != result {
		t.Errorf("expected %v received %v", val, result)
	}
}

func TestEncoder_Time(t *testing.T) {
	t.Parallel()
	e := NewEncoder()

	n, err := e.Time(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if n != LongSize {
		t.Errorf("expected %d got %d", LongSize, n)
	}

	d := NewDecoder(e)
	result, err := d.Time()
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsZero() {
		t.Error("timestamp round trip failed. Want zero-value have non-zero")
	}

	e.Reset()
	ts := time.Unix(-10000, 0)
	_, err = e.Time(ts)
	if err != nil {
		t.Fatal(err)
	}
	d.SetReader(e)
	result, err = d.Time()
	if err != nil {
		t.Fatal(err)
	}
	if result != ts {
		t.Errorf("timestamp round trip failed, expected %s got %s",
			ts.String(), result.String())
	}
}

func TestEncoder_PtrParam(t *testing.T) {
	f := 451.0
	e := NewEncoder()
	_, err := e.Marshal(&f)
	if err != nil {
		t.Fatal(err)
	}
	expLen := 9
	if e.Len() != expLen {
		t.Fatalf("expected %d got %d", expLen, e.Len())
	}

	a := NewDecoderAt(bytes.NewReader(e.Bytes()))
	v, err := a.ByteAt(0)
	if err != nil {
		t.Fatal(err)
	}
	if int8(v) != FloatColumn {
		t.Errorf("expected %v got %v", FloatColumn, v)
	}

	fv, err := a.Float64At(1)
	if err != nil {
		t.Fatal(err)
	}
	if fv != f {
		t.Errorf("expected %v got %v", f, fv)
	}
}

func TestEncoder_IntArrayParam(t *testing.T) {
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

	var offset int64
	a := NewDecoderAt(bytes.NewReader(e.Bytes()))
	v, err := a.ByteAt(offset)
	if err != nil {
		t.Fatal(err)
	}
	if int8(v) != ArrayColumn {
		t.Errorf("expected %v got %v", ArrayColumn, v)
	}
	offset++

	i, err := a.Int16At(offset)
	if err != nil {
		t.Fatal(err)
	}
	if i != 3 {
		t.Errorf("expected %v got %v", 3, i)
	}
	offset += 2
	for _, exp := range array {
		v, err = a.ByteAt(offset)
		if err != nil {
			t.Fatal(err)
		}
		if int8(v) != IntColumn {
			t.Errorf("expected %v got %v", IntColumn, v)
		}
		offset++

		iv, err := a.Int32At(offset)
		if err != nil {
			t.Fatal(err)
		}
		if iv != exp {
			t.Errorf("expected %v got %v", exp, iv)
		}
		offset += 4
	}
}

func TestEncoder_StringSliceParam(t *testing.T) {
	array := []string{
		"zero", "one",
		"two", "three",
		"four", "five",
		"six", "seven",
		"eight", "nine",
		"ten", "eleven",
		"twelve", "thirteen",
		"fourteen", "fifteen",
		"sixteen", "seventeen",
		"eighteen", "nineteen",
	}
	expLen := 213
	e := NewEncoder()
	_, err := e.Marshal(array)
	if err != nil {
		t.Fatal(err)
	}
	if e.Len() != expLen {
		t.Fatalf("expected %d got %d", expLen, e.Len())
	}

	var offset int64
	a := NewDecoderAt(bytes.NewReader(e.Bytes()))
	v, err := a.ByteAt(offset)
	if err != nil {
		t.Fatal(err)
	}
	if int8(v) != ArrayColumn {
		t.Errorf("expected %v got %v", ArrayColumn, v)
	}
	offset++

	i, err := a.Int16At(offset)
	if err != nil {
		t.Fatal(err)
	}
	if i != 20 {
		t.Errorf("expected %v got %v", 20, i)
	}
	offset += 2
	for _, exp := range array {
		v, err = a.ByteAt(offset)
		if err != nil {
			t.Fatal(err)
		}
		if int8(v) != StringColumn {
			t.Errorf("expected %v got %v", StringColumn, v)
		}
		offset++

		iv, err := a.StringAt(offset)
		if err != nil {
			t.Fatal(err)
		}
		if iv != exp {
			t.Errorf("expected %v got %v", exp, iv)
		}
		offset += int64((4 + len(exp)))
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

	var offset int64
	a := NewDecoderAt(bytes.NewReader(e.Bytes()))
	v, err := a.ByteAt(offset)
	if err != nil {
		t.Fatal(err)
	}
	if int8(v) != ArrayColumn {
		t.Errorf("expected %v got %v", ArrayColumn, v)
	}
	offset++

	i, err := a.Int16At(offset)
	if err != nil {
		t.Fatal(err)
	}
	if i != 3 {
		t.Errorf("expected %v got %v", 3, i)
	}
	offset += 2
	for _, exp := range array {
		v, err = a.ByteAt(offset)
		if err != nil {
			t.Fatal(err)
		}
		if int8(v) != FloatColumn {
			t.Errorf("expected %v got %v", FloatColumn, v)
		}
		offset++

		iv, err := a.Float64At(offset)
		if err != nil {
			t.Fatal(err)
		}
		if iv != exp {
			t.Errorf("expected %v got %v", exp, iv)
		}
		offset += 8
	}
}

func TestEncoder_Login(t *testing.T) {
	e := NewEncoder()
	v, err := e.Login(1, "hello", "world")
	if err != nil {
		t.Fatal(err)
	}
	fileBytes, err := ioutil.ReadFile("./fixture/authentication_request_sha256.msg")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, fileBytes) {
		t.Fatal("login message doesn't match expected contents")
	}
}
