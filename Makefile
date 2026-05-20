.PHONY: format build-http-map-example build-run-http-map-example build-run

format:
	@go fmt ./...

build-http-map-example:
	@go build -o bin/http_map_server examples/http_map/server/main.go
	@go build -o bin/http_map_client examples/http_map/client/main.go

build-run-http-map-example:
	@make build-http-map-example
	@./bin/http_map_server

build-run:
	@make build-run-http-map-example
