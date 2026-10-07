. "$PSScriptRoot/env.ps1"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.8
if ($LASTEXITCODE) { throw 'protoc-gen-go installation failed' }
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
if ($LASTEXITCODE) { throw 'protoc-gen-go-grpc installation failed' }
protoc --go_out=. --go_opt=module=streamforge --go-grpc_out=. --go-grpc_opt=module=streamforge api/proto/streamforge.proto
if ($LASTEXITCODE) { throw 'protobuf generation failed' }
