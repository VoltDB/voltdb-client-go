bench:
	go test -run none -v  -bench=. ./voltdbclient

test:
	go test -v  ./voltdbclient