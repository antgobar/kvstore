.PHONY: test format build-http-map-example build-run-http-map-example build-run gen-proto

test:
	@go test ./pkg/integration

format:
	@go fmt ./...

build-http-map-example:
	@go build -o bin/http_map_server examples/http_map/server/main.go
	@go build -o bin/http_map_client examples/http_map/client/main.go

build-run-http-map-example:
	@make build-http-map-example
	@./bin/http_map_server

build-grpc-map-example:
	@go build -o bin/grpc_map_server examples/grpc_map/server/main.go
	@go build -o bin/grpc_map_client examples/grpc_map/client/main.go

build-run-grpc-map-example:
	@make build-grpc-map-example
	@./bin/grpc_map_server

build:
	@make build-http-map-example
	@make build-grpc-map-example

gen-proto:
	@./proto/generate_proto.sh