.PHONY: build clean

build:
	go mod download
	xk6 build --with github.com/mkutlak/xk6-output-clickhouse=.

clean:
	rm -f k6
