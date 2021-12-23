.PHONY: compile
compile:
	protoc $(shell find . -type f -name *.proto) \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: test
test:
	go test -race ./...
