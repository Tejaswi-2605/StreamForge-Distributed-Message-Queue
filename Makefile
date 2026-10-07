.PHONY: build generate test integration race bench demo
build:
	@mkdir -p bin
	@for name in broker admin producer consumer benchmark replay demo; do go build -buildvcs=false -trimpath -o bin/streamforge-$$name ./cmd/$$name || exit 1; done
generate:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.8
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	PATH="$$(go env GOPATH)/bin:$$PATH" protoc --go_out=. --go_opt=module=streamforge --go-grpc_out=. --go-grpc_opt=module=streamforge api/proto/streamforge.proto
test:
	go test -count=1 ./...
integration:
	STREAMFORGE_INTEGRATION=1 go test -count=1 ./tests/integration
race:
	STREAMFORGE_INTEGRATION=1 go test -race -count=1 ./...
bench:
	go test -run '^$$' -bench . -benchmem ./internal/logstore
demo:
	docker compose up -d --build --wait
	docker compose --profile tools run --rm tools
