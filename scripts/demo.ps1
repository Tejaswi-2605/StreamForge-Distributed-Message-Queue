. "$PSScriptRoot/env.ps1"
docker compose up -d --build --wait
if ($LASTEXITCODE) { throw 'Compose startup failed' }
docker compose --profile tools run --rm tools
if ($LASTEXITCODE) { throw 'Demo assertions failed' }
docker compose ps
