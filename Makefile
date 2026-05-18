.PHONY: format run-http-serve

format:
	@go fmt ./...

build-http:
	@go build -o bin/http_server cmd/http_server/main.go
	@go build -o bin/http_client cmd/http_client/main.go

build-run-http:
	@make build-http
	@./bin/http_server