.PHONY: format run-http-serve

format:
	@go fmt ./...

run-http-server:
	@go run cmd/http_server/main.go
