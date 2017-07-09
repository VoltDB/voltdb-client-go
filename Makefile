test:
	go test -v ./wire
	go test -v  ./voltdbclient

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