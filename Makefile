.PHONY: format run-http-serve

format:
	@go fmt ./...

run-http-server:
	@go run cmd/http_server/main.go


build-http-example:
	@go build -o bin/http_server cmd/http_server/main.go
	@go build -o bin/http_client cmd/http_client/main.go