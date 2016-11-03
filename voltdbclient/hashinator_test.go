package voltdbclient

import (
	"crypto/rand"
	"database/sql/driver"
	"io/ioutil"
	r "math/rand"
	"testing"
)

var result int
var jsonBytes []byte
var h hashinator

func BenchmarkHashinater_getHashedPartitionForParameter_int32(b *testing.B) {
	var hashedPartition int
	jsonBytes, _ := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	h, _ := newHashinatorElastic(JSONFormat, true, jsonBytes)
	partitionParameterType := int(VTInt)
	partitionValue := driver.Value(r.Int31())
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		result = hashedPartition
	}
}

func BenchmarkHashinater_getHashedPartitionForParameter_int64(b *testing.B) {
	var hashedPartition int
	jsonBytes, _ := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	h, _ := newHashinatorElastic(JSONFormat, true, jsonBytes)
	partitionParameterType := int(VTLong)
	partitionValue := driver.Value(r.Int63())
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		result = hashedPartition
	}
}

func BenchmarkHashinater_getHashedPartitionForParameter_String(b *testing.B) {
	var hashedPartition int
	jsonBytes, _ := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	h, _ := newHashinatorElastic(JSONFormat, true, jsonBytes)
	partitionParameterType := int(VTString)
	partitionValue := driver.Value("123456789012345")
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		result = hashedPartition
	}
}

func BenchmarkHashinater_getHashedPartitionForParameter_Bytes(b *testing.B) {
	var hashedPartition int
	jsonBytes, _ := ioutil.ReadFile("./test_resources/jsonConfigC.bin")
	h, _ := newHashinatorElastic(JSONFormat, true, jsonBytes)
	valueToHash := make([]byte, 1000)
	_, _ = rand.Read(valueToHash)
	partitionParameterType := int(VTVarBin)
	partitionValue := driver.Value(valueToHash)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		result = hashedPartition
	}
}
