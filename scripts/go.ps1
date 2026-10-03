# Runs the Go toolchain inside Docker so the host needs no Go install.
$root = (Resolve-Path "$PSScriptRoot\..").Path -replace '\\', '/'
$name = "kafka-go-go-" + [guid]::NewGuid().ToString('N').Substring(0, 8)
docker run --rm --name $name -v "${root}:/src" -v kafka-go-gomod:/go/pkg/mod -v kafka-go-gobuild:/root/.cache/go-build -w /src golang:1.26 go @args
exit $LASTEXITCODE
