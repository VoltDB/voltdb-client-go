package voltdbclient

import (
	"crypto/rand"
	"database/sql/driver"
	"io/ioutil"
	r "math/rand"
	"testing"

	"github.com/VoltDB/voltdb-client-go/wire"
)

func BenchmarkHashinater_getHashedPartitionForParameter_int32(b *testing.B) {
	jsonBytes, err := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	if err != nil {
		b.Fatal(err)
	}
	h, err := newHashinatorElastic(JSONFormat, true, jsonBytes)
	if err != nil {
		b.Fatal(err)
	}
	pType := int(wire.IntColumn)
	pVal := driver.Value(r.Int31())
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, err = h.getHashedPartitionForParameter(pType, pVal)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashinater_getHashedPartitionForParameter_int64(b *testing.B) {
	jsonBytes, err := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	if err != nil {
		b.Fatal(err)
	}
	h, err := newHashinatorElastic(JSONFormat, true, jsonBytes)
	if err != nil {
		b.Fatal(err)
	}
	pTyp := int(wire.LongColumn)
	pVal := driver.Value(r.Int63())
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err = h.getHashedPartitionForParameter(pTyp, pVal)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashinater_getHashedPartitionForParameter_String(b *testing.B) {
	jsonBytes, err := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	if err != nil {
		b.Fatal(err)
	}
	h, err := newHashinatorElastic(JSONFormat, true, jsonBytes)
	if err != nil {
		b.Fatal(err)
	}
	pType := int(wire.StringColumn)
	pVal := driver.Value("123456789012345")
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err = h.getHashedPartitionForParameter(pType, pVal)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashinater_getHashedPartitionForParameter_Bytes(b *testing.B) {
	jsonBytes, err := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	if err != nil {
		b.Fatal(err)
	}
	h, err := newHashinatorElastic(JSONFormat, true, jsonBytes)
	if err != nil {
		b.Fatal(err)
	}
	valueToHash := make([]byte, 1000)
	_, _ = rand.Read(valueToHash)
	pType := int(wire.VarBinColumn)
	pVal := driver.Value(valueToHash)
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err = h.getHashedPartitionForParameter(pType, pVal)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestHashinator(t *testing.T) {
	jsonBytes, err := ioutil.ReadFile("./test_resources/configBytes.bytes")
	if err != nil {
		t.Fatal(err)
	}
	h, err := newHashinatorElastic(JSONFormat, true, jsonBytes)
	if err != nil {
		t.Fatal(err)
	}
	sample := []int{2, 2, 1, 0, 1, 2, 2, 0, 0, 2}
	for i := 0; i < 10; i++ {
		partition, err := h.getHashedPartitionForParameter(0, int32(i))
		if err != nil {
			t.Fatal(err)
		}
		expect := sample[i]
		if partition != expect {
			t.Errorf("id: %d expected %d got %d", i, expect, partition)
		}
	}
}
