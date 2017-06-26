package voltdbclient

import (
	"bytes"
	"crypto/rand"
	"database/sql/driver"
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
		v := s[k]
		r.Reset(v)
		b.StartTimer()
		_, err = deserializeResponse(r, h)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func loadQueryResponseSamples() ([][]byte, int64, error) {
	return loadSamples("query")
}

func loadExecResponseSamples() ([][]byte, int64, error) {
	return loadSamples("exec")
}

// This function loads deserialization sample files which are found in the
// test_resource/deserialize directory.
//
// from can either be "query" or "exec" which loads query and exec samples
// respectively
//
// The following steps were taken to generate the sample found in
// `test_resources/deserialize/` directory.
//
// First we create a database table witht the following schema
//
// ```sql
// create table deserialize(
// 	tiny TINYINT,
// 	short SMALLINT,
// 	int INTEGER,
// 	long BIGINT,
// 	double FLOAT,
// 	string VARCHAR,
// 	byte_array VARBINARY,
// 	time TIMESTAMP,
// )
// ```

// Then 1000 records are inserted with random data.

// And, we query the database with increasing limit of 100 i.e

// ```sql SELECT tiny,
//        short,
//        INT,
//        LONG,
//        DOUBLE,
//        string,
//        byte_array,
//        time
// FROM   deserialize LIMIT  ?;
// ```
// with  arguments  100, 200, 300, 400, 500, 600, 700, 800, 900, 1000
//
// We then save the blob response( after taking care of decoding the messagge
// header and properly read the message0 into a file `{limit}.bin`. The `handle`
// file is just a helper storing the `request/response`  handle  id. This is for
// `test_resource/deserialize/query`.
//
// For `test_resource/deserialize/query` , we first create 10 tables with the
// following schema ```sql create table deserialize_{tableNumber}(
// 	tiny TINYINT,
// 	short SMALLINT,
// 	int INTEGER,
// 	long BIGINT,
// 	double FLOAT,
// 	string VARCHAR,
// 	byte_array VARBINARY,
// 	time TIMESTAMP,
// )
// ```
// where by `tableNumber` is the number of the table in words ie one, two,
// three, four ... ten. Each table is filled with 100 records.
//
// Then we create another table with the following schema ```sql create table
// deserialize_exec(
// 	tiny TINYINT,
// 	short SMALLINT,
// 	int INTEGER,
// 	long BIGINT,
// 	double FLOAT,
// 	string VARCHAR,
// 	byte_array VARBINARY,
// 	time TIMESTAMP,
// )
// ```
//
// Our goal is to capture the raw bytes returned by the voltdb when executing
// Exec commands.
//
// Then for each value in the sequence 100, 200, 300, 400, 500, 600, 700, 800,
// 900, 1000 . we execute the following SQL command
//
// ```sql INSERT INTO deserialize_exec SELECT * FROM deserialize_{tableName} ;`
// ```
// where `tablename` is the index position of the sequence in words startung
// from one i.e
//  sequence | index | tableName
// ----------|-------|-------------
// 100       |  0    | one
// 200       |  1    | two
// 300       |  2    | three
// 400       |  3    | four
// 500       |  4    | five
// 600       |  5    | six
// 700       |  6    | seven
// 800       |  7    | eight
// 900       |  8    | nine
// 1000      |  9    | ten
//
// We then save the blob response( after taking care of decoding the messagge
// header and properly read the message0 into a file `{sequence}.bin`. The `handle`
// file is just a helper storing the `request/response`  handle  id. This is for
// `test_resource/deserialize/query`.
//
func loadSamples(from string) ([][]byte, int64, error) {
	var out [][]byte
	var handle int64
	dir := filepath.Join("./test_resources/deserialize", from)
	ferr := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		ext := filepath.Ext(p)
		if ext != ".bin" {
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
		key := queryResponseSampleKey()
		r.Reset(s[key])
		res, err = deserializeResponse(r, h)
		if err != nil {
			b.Fatal(err, key)
		}
		b.StartTimer()
		_, err = deserializeRows(r, res)
		if err != nil {
			b.Fatal(err, key)
		}
	}
}

func BenchmarkDeserializeResult(b *testing.B) {
	s, h, err := loadExecResponseSamples()
	if err != nil {
		b.Fatal(err)
	}
	var res voltResponse
	r := bytes.NewReader([]byte{})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		k := queryResponseSampleKey()
		v := s[k]
		r.Reset(v)
		res, err = deserializeResponse(r, h)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = deserializeResult(r, res)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExec(b *testing.B) {
	schema := `
create table bench_exec(
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
		b.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec("@AdHoc", `drop table bench_exec if exists ;`)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		_, err = db.Exec("@AdHoc", `drop table bench_exec if exists ;`)
		if err != nil {
			b.Fatal(err)
		}
	}()

	_, err = db.Exec("@AdHoc", schema)
	if err != nil {
		b.Fatal(err)
	}
	stmtStr := `
INSERT INTO bench_exec (tiny,short,int,long,double,string,byte_array,time)
VALUES (?,?,?,?,?,?,?,?);`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, bb, c, d := int8(1), int16(1), int32(1), int64(1)
		e, f, g, h := float64(1), stringArg(i), byteSliceArg(i), time.Now()
		b.StartTimer()
		_, err = db.Exec("@AdHoc", stmtStr, a, bb, c, d, e, f, g, h)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPrepareStmtExec(b *testing.B) {
	schema := `
create table bench_prepare_exec(
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
		b.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec("@AdHoc", `drop table bench_prepare_exec if exists ;`)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		_, err = db.Exec("@AdHoc", `drop table bench_prepare_exec if exists ;`)
		if err != nil {
			b.Fatal(err)
		}
	}()

	_, err = db.Exec("@AdHoc", schema)
	if err != nil {
		b.Fatal(err)
	}
	stmtStr := `
INSERT INTO bench_prepare_exec (tiny,short,int,long,double,string,byte_array,time)
VALUES (?,?,?,?,?,?,?,?);`
	st, err := db.Prepare(stmtStr)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, bb, c, d := int8(1), int16(1), int32(1), int64(1)
		e, f, g, h := float64(1), stringArg(i), byteSliceArg(i), time.Now()
		b.StartTimer()
		_, err = st.Exec(a, bb, c, d, e, f, g, h)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	schema := `
create table bench_query(
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
		b.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec("@AdHoc", `drop table bench_query if exists ;`)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("@AdHoc", `drop table bench_query if exists ;`)
		if err != nil {
			b.Fatal(err)
		}
	}()

	_, err = db.Exec("@AdHoc", schema)
	if err != nil {
		b.Fatal(err)
	}
	stmtStr := `
INSERT INTO bench_query (tiny,short,int,long,double,string,byte_array,time)
VALUES (?,?,?,?,?,?,?,?);`
	st, err := db.Prepare(stmtStr)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		_, err = st.Exec(
			int8(1), int16(1), int32(1), int64(1), float64(1),
			stringArg(i), byteSliceArg(i), time.Now(),
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	limits := []int{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	qst := "select * from bench_query limit ?"
	var rows *sql.Rows
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		k := queryResponseSampleKey()
		l := limits[k]
		b.StartTimer()
		rows, err = db.Query("@AdHoc", qst, l)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkPrepareStmtQuery(b *testing.B) {
	schema := `
create table bench_prepare_query(
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
		b.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec("@AdHoc", `drop table bench_prepare_query if exists ;`)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("@AdHoc", `drop table bench_prepare_query if exists ;`)
		if err != nil {
			b.Fatal(err)
		}
	}()

	_, err = db.Exec("@AdHoc", schema)
	if err != nil {
		b.Fatal(err)
	}
	stmtStr := `
INSERT INTO bench_prepare_query (tiny,short,int,long,double,string,byte_array,time)
VALUES (?,?,?,?,?,?,?,?);`
	st, err := db.Prepare(stmtStr)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		_, err = st.Exec(
			int8(1), int16(1), int32(1), int64(1), float64(1),
			stringArg(i), byteSliceArg(i), time.Now(),
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	limits := []int{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	qst := "select * from bench_prepare_query limit ?"
	pst, err := db.Prepare(qst)
	if err != nil {
		b.Fatal(err)
	}
	var rows *sql.Rows
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		k := queryResponseSampleKey()
		l := limits[k]
		b.StartTimer()
		rows, err = pst.Query(l)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}
