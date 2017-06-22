test:
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

# Installs benchcmp tool
deps:
	go get golang.org/x/tools/cmd/benchcmp
