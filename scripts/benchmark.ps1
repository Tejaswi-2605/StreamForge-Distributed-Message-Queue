param(
    [string]$PostgresDSN = 'postgres://streamforge@127.0.0.1:55432/streamforge?sslmode=disable',
    [string]$RedisAddress = '127.0.0.1:56379',
    [string]$ReplayArchive = ''
)
. "$PSScriptRoot/env.ps1"
$psql = Join-Path $projectRoot '.tools/pgsql/bin/psql.exe'
if (!(Test-Path -LiteralPath $psql)) {
    $psql = (Get-Command psql -ErrorAction Stop).Source
}
$benchmarkBinary = Join-Path $projectRoot 'bin/streamforge-benchmark.exe'
if (!(Test-Path -LiteralPath $benchmarkBinary)) { throw 'Run scripts/build.ps1 first.' }
$runId = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0,8)
$reportDir = Join-Path $projectRoot "artifacts/benchmarks/$runId"
$dataRoot = Join-Path $projectRoot "data/brokers/benchmarks/$runId"
New-Item -ItemType Directory -Force $reportDir,$dataRoot | Out-Null
$script:usedPorts = New-Object 'System.Collections.Generic.HashSet[int]'

function Get-FreePort {
    do {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
        $listener.Start()
        $port = $listener.LocalEndpoint.Port
        $listener.Stop()
    } until ($script:usedPorts.Add($port))
    return $port
}

function Stop-BenchmarkCluster($state) {
    if (!$state) { return }
    foreach ($process in $state.Processes) {
        if (!$process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }
    $priorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $psql --dbname=$PostgresDSN -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS $($state.Schema) CASCADE" 2> (Join-Path $reportDir 'schema-cleanup.log') | Out-Null
        $cleanupExit = $LASTEXITCODE
    } finally { $ErrorActionPreference=$priorPreference }
    if ($cleanupExit) { throw 'Benchmark schema cleanup failed.' }
}

function Start-BenchmarkCluster([int]$count,[string]$durability) {
    $schema = 'bench_' + [guid]::NewGuid().ToString('N')
    & $psql --dbname=$PostgresDSN -v ON_ERROR_STOP=1 -c "CREATE SCHEMA $schema" | Out-Null
    if ($LASTEXITCODE) { throw 'Could not create isolated benchmark schema.' }
    $databaseUrl = [UriBuilder]::new($PostgresDSN)
    $query = $databaseUrl.Query.TrimStart('?')
    $databaseUrl.Query = $query + '&search_path=' + $schema
    $addresses = @()
    $metricsAddresses = @()
    foreach ($number in 1..$count) {
        $addresses += '127.0.0.1:' + (Get-FreePort)
        $metricsAddresses += '127.0.0.1:' + (Get-FreePort)
    }
    $clusterDir = Join-Path $dataRoot $schema
    $state = [pscustomobject]@{Processes=@(); Schema=$schema; DataDir=$clusterDir; Address=$addresses[0]; Count=$count; Durability=$durability}
    try {
        $env:POSTGRES_DSN = $databaseUrl.Uri.AbsoluteUri
        $env:REDIS_ADDR = $RedisAddress
        $env:BROKER_LIST = $addresses -join ','
        $env:FSYNC_MODE = $durability
        $env:RETENTION_MAX_SEGMENTS = '0'
        $env:RETENTION_INTERVAL = '1m'
        $env:MAX_MESSAGE_SIZE = '1048576'
        $env:SEGMENT_MAX_BYTES = '16777216'
        $env:INDEX_INTERVAL = '64'
        $env:MAX_INFLIGHT_REQUESTS = '128'
        $env:MAX_BATCH_SIZE = '100'
        $env:MAX_FETCH_SIZE = '100'
        $env:LEASE_TTL = '30s'
        $env:MAX_RETRIES = '3'
        $env:RETRY_BASE = '1s'
        $env:RETRY_MAX = '1m'
        foreach ($id in 0..($count-1)) {
            $env:BROKER_ID = "$id"
            $env:BROKER_ADDRESS = $addresses[$id]
            $env:METRICS_ADDRESS = $metricsAddresses[$id]
            $env:DATA_DIR = Join-Path $clusterDir "broker-$id"
            $state.Processes += Start-Process -FilePath (Join-Path $projectRoot 'bin/streamforge-broker.exe') -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $reportDir "$schema-$id.stdout.log") -RedirectStandardError (Join-Path $reportDir "$schema-$id.stderr.log")
        }
        foreach ($address in $metricsAddresses) {
            $healthy = $false
            foreach ($attempt in 1..80) {
                try {
                    $response = Invoke-WebRequest -UseBasicParsing -Uri "http://$address/healthz" -TimeoutSec 1
                    if ($response.StatusCode -eq 200) { $healthy=$true; break }
                } catch {}
                Start-Sleep -Milliseconds 250
            }
            if (!$healthy) { throw "Benchmark broker health failed at $address" }
        }
        return $state
    } catch {
        Stop-BenchmarkCluster $state
        throw
    }
}

function Get-LogBytes([string]$path) {
    if (!(Test-Path -LiteralPath $path)) { return [long]0 }
    $total = [long]0
    foreach ($file in Get-ChildItem -LiteralPath $path -Recurse -File -Filter '*.log') {
        # Windows directory metadata can report stale lengths while a broker
        # keeps its file open. Query the current EOF through a shared handle.
        $stream = [IO.File]::Open($file.FullName,[IO.FileMode]::Open,[IO.FileAccess]::Read,([IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete))
        try { $total += $stream.Length } finally { $stream.Dispose() }
    }
    return $total
}

# A coverage matrix, not a full Cartesian product. Large cases use write mode;
# separate smaller fsync cases measure that different acknowledgment contract.
$trials = @(
    @{Name='raw-small'; Mode='raw'; Messages=10000; Size=100; Partitions=1; Cluster=0; Durability='write'},
    @{Name='raw-medium'; Mode='raw'; Messages=100000; Size=1024; Partitions=3; Cluster=0; Durability='write'},
    @{Name='raw-large'; Mode='raw'; Messages=500000; Size=100; Partitions=10; Cluster=0; Durability='write'},
    @{Name='raw-fsync'; Mode='raw'; Messages=10000; Size=10240; Partitions=1; Cluster=0; Durability='fsync'},
    @{Name='api-single'; Mode='api'; Messages=10000; Size=100; Partitions=1; Cluster=1; Durability='fsync'},
    @{Name='api-medium'; Mode='api'; Messages=100000; Size=1024; Partitions=3; Cluster=3; Durability='write'},
    @{Name='api-large'; Mode='api'; Messages=500000; Size=100; Partitions=10; Cluster=3; Durability='write'},
    @{Name='api-fsync'; Mode='api'; Messages=10000; Size=10240; Partitions=3; Cluster=3; Durability='fsync'}
)
$state = $null
$observations = @()
try {
    foreach ($trial in $trials) {
        Write-Output "$($trial.Name) START messages=$($trial.Messages) payload=$($trial.Size) partitions=$($trial.Partitions) brokers=$($trial.Cluster) durability=$($trial.Durability)"
        $address = '127.0.0.1:9001'
        $bytesBefore = [long]0
        if ($trial.Mode -eq 'api') {
            if (!$state -or $state.Count -ne $trial.Cluster -or $state.Durability -ne $trial.Durability) {
                Stop-BenchmarkCluster $state
                $state = $null
                $state = Start-BenchmarkCluster $trial.Cluster $trial.Durability
            }
            $address = $state.Address
            $bytesBefore = Get-LogBytes $state.DataDir
        }
        $resultPath = Join-Path $reportDir "$($trial.Name).json"
        & $benchmarkBinary --broker $address --mode $trial.Mode --messages $trial.Messages --size $trial.Size --partitions $trial.Partitions --durability $trial.Durability | Set-Content -LiteralPath $resultPath -Encoding UTF8
        if ($LASTEXITCODE) { throw "$($trial.Name) benchmark failed." }
        $result = Get-Content -LiteralPath $resultPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($result.Messages -ne $trial.Messages -or $result.Brokers -ne $trial.Cluster) { throw 'Benchmark workload/owner mismatch.' }
        if ($trial.Mode -eq 'api') {
            $result | Add-Member -NotePropertyName ObservedLogBytes -NotePropertyValue ((Get-LogBytes $state.DataDir) - $bytesBefore)
            if ($result.ObservedLogBytes -lt $result.PayloadTotalBytes) { throw 'Observed log size is below published payload bytes.' }
        }
        $result | Add-Member -NotePropertyName Trial -NotePropertyValue $trial.Name
        $observations += $result
        $observations | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $reportDir 'matrix.json') -Encoding UTF8
        Write-Output "$($trial.Name) PASSED publish_per_second=$($result.PublishPerSecond) p99_ms=$($result.P99MS)"
    }
    if ($ReplayArchive) {
        if (!$state -or $state.Count -ne 3 -or $state.Durability -ne 'fsync') {
            Stop-BenchmarkCluster $state
            $state = $null
            $state = Start-BenchmarkCluster 3 'fsync'
        }
        $topic = 'archive.' + [guid]::NewGuid().ToString('N')
        & ./bin/streamforge-admin.exe create-topic --broker $state.Address --topic $topic --partitions 3 | Out-Null
        if ($LASTEXITCODE) { throw 'Replay topic creation failed.' }
        & ./bin/streamforge-replay.exe --broker $state.Address --file $ReplayArchive --topic $topic --limit 0 | Set-Content -LiteralPath (Join-Path $reportDir 'replay.json') -Encoding UTF8
        if ($LASTEXITCODE) { throw 'Authentic group replay failed.' }
        Write-Output 'GROUP_REPLAY_PASSED'
    }
    Write-Output "VERIFIED_REPORT_DIRECTORY=$reportDir"
} finally {
    Stop-BenchmarkCluster $state
}
