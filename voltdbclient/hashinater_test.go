package voltdbclient

import (
	"testing"
	"io/ioutil"
	"crypto/rand"
	r "math/rand"
	"database/sql/driver"
	"fmt"
)


var result int

func BenchmarkHashinater_getHashedPartitionForParameter_int32(b *testing.B) {
	var hashedPartition int
	jsonBytes,_ := ioutil.ReadFile("/home/qwang/jsonConfigC.bin")
	h, _ := newHashinaterElastic(JSON_FORMAT, true, jsonBytes)
	partitionParameterType := 2
	partitionValue := driver.Value(r.Int31())
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		//fmt.Println(valueToHash, hashedPartition)
		result = hashedPartition
	}
	fmt.Println(result)
}

func BenchmarkHashinater_getHashedPartitionForParameter_int64(b *testing.B) {
	var hashedPartition int
	jsonBytes,_ := ioutil.ReadFile("/home/qwang/jsonConfigC.bin")
	h, _ := newHashinaterElastic(JSON_FORMAT, true, jsonBytes)
	partitionParameterType := 3
	partitionValue := driver.Value(r.Int63())
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		//fmt.Println(valueToHash, hashedPartition)
		result = hashedPartition
	}
	fmt.Println(result)
}

func BenchmarkHashinater_getHashedPartitionForParameter_String(b *testing.B) {
	var hashedPartition int
	jsonBytes,_ := ioutil.ReadFile("/home/qwang/jsonConfigC.bin")
	h, _ := newHashinaterElastic(JSON_FORMAT, true, jsonBytes)
	partitionParameterType := 6
	partitionValue := driver.Value("123456789012345")
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		//fmt.Println(valueToHash, hashedPartition)
		result = hashedPartition
	}
	fmt.Println(result)
}

func BenchmarkHashinater_getHashedPartitionForParameter_Bytes(b *testing.B) {
	var hashedPartition int
	jsonBytes,_ := ioutil.ReadFile("/home/qwang/jsonConfigC.bin")
	h, _ := newHashinaterElastic(JSON_FORMAT, true, jsonBytes)
	valueToHash := make([]byte, 1000)
	_, _ = rand.Read(valueToHash)
	partitionParameterType := 1
	partitionValue := driver.Value(valueToHash)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		hashedPartition, _ = h.getHashedPartitionForParameter(partitionParameterType, partitionValue)
		//fmt.Println(valueToHash, hashedPartition)
		result = hashedPartition
	}
	fmt.Println(result)
}