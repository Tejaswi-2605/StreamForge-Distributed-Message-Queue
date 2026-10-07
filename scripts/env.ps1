$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot
$localGo = Join-Path $projectRoot '.tools/go/bin'
if (Test-Path -LiteralPath $localGo) { $env:PATH = "$localGo;$env:PATH" }
$env:GOPATH = Join-Path $projectRoot '.tools/gopath'
$env:GOCACHE = Join-Path $projectRoot '.tools/gocache'
$env:GOBIN = Join-Path $projectRoot '.tools/bin'
$env:PATH = "$env:GOBIN;$env:PATH"
$localDockerBin = Join-Path $env:LOCALAPPDATA 'Programs/DockerDesktop/resources/bin'
if (Test-Path -LiteralPath (Join-Path $localDockerBin 'docker.exe')) { $env:PATH = "$localDockerBin;$env:PATH" }
