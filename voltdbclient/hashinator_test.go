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
