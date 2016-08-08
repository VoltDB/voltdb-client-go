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
	h, _ := newHashinatorElastic(JSON_FORMAT, true, jsonBytes)
	partitionParameterType := int(VT_INT)
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
	h, _ := newHashinatorElastic(JSON_FORMAT, true, jsonBytes)
	partitionParameterType := int(VT_LONG)
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
	h, _ := newHashinatorElastic(JSON_FORMAT, true, jsonBytes)
	partitionParameterType := int(VT_STRING)
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
	h, _ := newHashinatorElastic(JSON_FORMAT, true, jsonBytes)
	valueToHash := make([]byte, 1000)
	_, _ = rand.Read(valueToHash)
	partitionParameterType := int(VT_VARBIN)
	partitionValue := driver.Value(valueToHash)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		result = hashedPartition
	}
}
