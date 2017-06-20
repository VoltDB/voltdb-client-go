package voltdbclient

import (
	"bytes"
	"crypto/rand"
	"database/sql/driver"
	"testing"
	"time"
)

func BenchmarkSerializeArgs(b *testing.B) {
	var buf bytes.Buffer
	var err error
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		buf.Reset()
		args := sampleSerialArgs(i)
		b.StartTimer()
		err = serializeArgs(&buf, args)
		if err != nil {
			b.Error(err)
		}
	}
}

func sampleSerialArgs(i int) []driver.Value {
	var args []driver.Value
	args = append(args, allintsArgs(i)...)
	args = append(args, stringArg(i))
	args = append(args, i > 1000)
	args = append(args, time.Now())

	args = append(args, []int8{int8(i)})
	args = append(args, []int16{int16(i)})
	args = append(args, []int32{int32(i)})
	args = append(args, []int64{int64(i)})
	args = append(args, []float64{float64(i)})
	args = append(args, []string{stringArg(i)})
	args = append(args, byteSliceArg(i))
	return args
}

func allintsArgs(i int) []driver.Value {
	return []driver.Value{
		int8(i), int16(i), int32(i), int64(i), float64(i),
	}
}

func stringArg(i int) string {
	return string(byteSliceArg(i))
}

func byteSliceArg(i int) []byte {
	b := make([]byte, 1000)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}
