test:
	go test -v ./wire
	go test -v  ./voltdbclient

test-race:
	go test -v -race ./wire
	go test -v -race ./voltdbclient

test-affinity:
	go test -v -run VerifyClientAffinity ./voltdbclient
bench:
	go test -run none -v  -bench=. ./voltdbclient

bench-old:
	go test -run none -bench=. ./voltdbclient >old.bench

bench-new:
	go test -run none -bench=. ./voltdbclient >new.bench

# This compares the output of running bench-old and bench-new rules
benchcmp:
	benchcmp old.bench new.bench

clean:
	rm -f *.out *.test

mem: clean
	go test -bench=. -memprofile=mem.out  ./voltdbclient
	go tool pprof -lines  -alloc_objects *.test mem.out

cpu: clean
	go test -bench=. -cpuprofile=cpu.out  ./voltdbclient
	go tool pprof -lines *.test cpu.out
# Installs benchcmp tool
deps:
	go get golang.org/x/tools/cmd/benchcmp

down:
	voltadmin shutdown

voter:
	go install ./examples/voter
	voter --runtype=sync  --servers="localhost:21212,localhost:21222,localhost:21232" --memprofile="mem.out"

voter-sql:
	go install ./examples/voter
	voter --runtype=sql --servers="localhost:21212,localhost:21222,localhost:21232" --memprofile="mem.out"

voter-async:
	go install ./examples/voter
	voter --runtype=async  --servers="localhost:21212,localhost:21222,localhost:21232" --memprofile="mem.out"
