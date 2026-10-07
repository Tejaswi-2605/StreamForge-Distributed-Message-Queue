param([ValidateSet('start','stop','status')][string]$Action='start')
. "$PSScriptRoot/env.ps1"
$pg = Join-Path $projectRoot '.tools/pgsql/bin'
$redis = Join-Path $projectRoot '.tools/redis/Redis-8.10.2-Windows-x64-msys2'
$dbdir = Join-Path $projectRoot 'data/local/postgres'
$pidfile = Join-Path $projectRoot 'data/local/redis.pid'
if ($Action -eq 'start') {
 if (!(Test-Path -LiteralPath "$pg/pg_ctl.exe")) { throw 'Portable PostgreSQL is missing; use Docker Compose or install local binaries.' }
 New-Item -ItemType Directory -Force data/local,artifacts | Out-Null
 if (!(Test-Path -LiteralPath "$dbdir/PG_VERSION")) {
  & "$pg/initdb.exe" -D $dbdir -U streamforge --auth=trust --encoding=UTF8 --locale=C
  if ($LASTEXITCODE) { throw 'initdb failed' }
 }
 & "$pg/pg_ctl.exe" -D $dbdir status
 if ($LASTEXITCODE) {
  & "$pg/pg_ctl.exe" -D $dbdir -l (Join-Path $projectRoot 'artifacts/postgres.log') -o '-p 55432 -h 127.0.0.1' -w start
  if ($LASTEXITCODE) { throw 'PostgreSQL start failed' }
 }
 $existing = & "$pg/psql.exe" -h 127.0.0.1 -p 55432 -U streamforge -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='streamforge'"
 if (!$existing) { & "$pg/createdb.exe" -h 127.0.0.1 -p 55432 -U streamforge streamforge; if ($LASTEXITCODE) { throw 'createdb failed' } }
 $priorPreference = $ErrorActionPreference
 $ErrorActionPreference = 'Continue'
 & "$redis/redis-cli.exe" -p 56379 ping 2>$null
 $redisResult = $LASTEXITCODE
 $ErrorActionPreference = $priorPreference
 if ($redisResult) {
  $p=Start-Process -FilePath "$redis/redis-server.exe" -ArgumentList @('--bind','127.0.0.1','--port','56379','--save','""','--appendonly','no') -WorkingDirectory (Join-Path $projectRoot 'data/local') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $projectRoot 'artifacts/redis.stdout.log') -RedirectStandardError (Join-Path $projectRoot 'artifacts/redis.stderr.log')
  Set-Content -LiteralPath $pidfile -Value $p.Id
 }
 $env:POSTGRES_DSN='postgres://streamforge@127.0.0.1:55432/streamforge?sslmode=disable'
 $env:REDIS_ADDR='127.0.0.1:56379'
 Write-Output 'Local DSN: postgres://streamforge@127.0.0.1:55432/streamforge?sslmode=disable; Redis: 127.0.0.1:56379'
} elseif ($Action -eq 'stop') {
 & "$redis/redis-cli.exe" -p 56379 shutdown nosave
 & "$pg/pg_ctl.exe" -D $dbdir -m fast -w stop
} else {
 & "$pg/pg_ctl.exe" -D $dbdir status
 & "$redis/redis-cli.exe" -p 56379 ping
}
