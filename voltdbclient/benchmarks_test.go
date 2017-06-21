package voltdbclient

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strconv"
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

func TestGenerateDeserialize(t *testing.T) {

	//WARNING: This is a helper testcase to create the table and populate with
	//data that is used to benchmark deserialization functions
	t.Skip()
	schema := `
create table deserialize(
	tiny TINYINT,
	short SMALLINT,
	int INTEGER,
	long BIGINT,
	double FLOAT,
	string VARCHAR,
	byte_array VARBINARY,
	time TIMESTAMP,
)
`
	db, err := sql.Open("voltdb", "localhost:21212")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("@AdHoc", schema)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare(`
insert into deserialize (tiny,short,int,long,double,string,byte_array,time)
 values (?,?,?,?,?,?,?,?);`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		_, err = stmt.Exec(
			int8(1), int16(1), int32(1), int64(1), float64(1),
			stringArg(i), byteSliceArg(i), time.Now(),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
}

type dStruct struct {
	tiny      byte
	short     int
	integer   int32
	long      int64
	double    float64
	varchar   string
	byteArray []byte
	time      time.Time
}

func TestDeserializeQueryBatches(t *testing.T) {
	t.Skip()
	db, err := sql.Open("voltdb", "localhost:21212")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare(`
	select tiny,short,int,long,double,string,byte_array,time
	from deserialize limit ?;`)
	if err != nil {
		t.Fatal(err)
	}
	dir := "test_resources/deserialize/"
	limits := []int{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	for _, v := range limits {
		os.Setenv("DES_BATCH", fmt.Sprintf("%s%d.bin", dir, v))
		rows, err := stmt.Query(v)
		if err != nil {
			t.Fatal(err)
		}
		var rst []dStruct
		for rows.Next() {
			v := dStruct{}
			err := rows.Scan(
				&v.tiny, &v.short,
				&v.integer, &v.long,
				&v.double,
				&v.varchar, &v.byteArray,
				&v.time,
			)
			if err != nil {
				t.Fatal(err)
			}
			rst = append(rst, v)
		}
		rows.Close()
	}
}

func BenchmarkDeserializeResponse(b *testing.B) {
	s, h, err := loadQueryResponseSamples()
	if err != nil {
		b.Fatal(err)
	}
	r := bytes.NewReader([]byte{})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		k := queryResponseSampleKey()
		r.Reset(s[k])
		b.StartTimer()
		_, err = deserializeResponse(r, h)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func loadQueryResponseSamples() ([][]byte, int64, error) {
	var out [][]byte
	var handle int64
	dir := "./test_resources/deserialize/query/"
	ferr := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		b, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}
		base := filepath.Base(p)
		if base == "handle" {
			v, err := strconv.Atoi(string(b))
			if err != nil {
				return err
			}
			handle = int64(v)
			return nil
		}
		out = append(out, b)
		return nil
	})
	if ferr != nil {
		return nil, 0, ferr
	}
	return out, handle, nil
}

func queryResponseSampleKey() int {
	return mrand.Intn(9)
}

func BenchmarkDeserializeRows(b *testing.B) {
	s, h, err := loadQueryResponseSamples()
	if err != nil {
		b.Fatal(err)
	}
	var res voltResponse
	r := bytes.NewReader([]byte{})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r.Reset(s[queryResponseSampleKey()])
		res, err = deserializeResponse(r, h)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = deserializeRows(r, res)
		if err != nil {
			b.Fatal(err)
		}
	}
}
