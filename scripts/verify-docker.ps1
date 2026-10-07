param([string]$DockerPath = 'docker')
. "$PSScriptRoot/env.ps1"
$dockerCommand = Get-Command $DockerPath -ErrorAction SilentlyContinue
if (!$dockerCommand -and $DockerPath -eq 'docker') {
    $DockerPath = Join-Path $env:LOCALAPPDATA 'Programs/DockerDesktop/resources/bin/docker.exe'
    $dockerCommand = Get-Command $DockerPath -ErrorAction Stop
}
if (!$dockerCommand) { throw "Docker executable unavailable: $DockerPath" }
$DockerPath = $dockerCommand.Source
$priorPath = $env:PATH
$env:PATH = (Split-Path -Parent $DockerPath) + ';' + $env:PATH
$runId = 'streamforge-check-' + [guid]::NewGuid().ToString('N').Substring(0,12)
$evidenceDir = Join-Path $projectRoot "artifacts/docker/$runId"
New-Item -ItemType Directory -Force $evidenceDir | Out-Null
$ports = [Collections.Generic.HashSet[int]]::new()
function Free-Port {
    do {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
        $listener.Start()
        $port = $listener.LocalEndpoint.Port
        $listener.Stop()
    } until ($ports.Add($port))
    return $port
}
# Separate project, volumes and random loopback ports avoid touching an existing stack.
$settings = [ordered]@{POSTGRES_PASSWORD=[guid]::NewGuid().ToString('N'); RETENTION_MAX_SEGMENTS='0'}
foreach ($name in @('POSTGRES_PORT','REDIS_PORT','BROKER_1_PORT','BROKER_2_PORT','BROKER_3_PORT','METRICS_1_PORT','METRICS_2_PORT','METRICS_3_PORT','PROMETHEUS_PORT')) { $settings[$name] = Free-Port }
$environmentFile = Join-Path $evidenceDir 'compose.env'
$settings.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" } | Set-Content -LiteralPath $environmentFile -Encoding ASCII
$composeArguments = @('compose','--project-name',$runId,'--env-file',$environmentFile,'-f',(Join-Path $projectRoot 'docker-compose.yml'))
function Compose([string[]]$Arguments) {
    $priorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $DockerPath @composeArguments @Arguments; $composeExit = $LASTEXITCODE } finally { $ErrorActionPreference = $priorPreference }
    if ($composeExit) { throw "Docker Compose failed: $Arguments" }
}
$priorEnvironment = @{}
foreach ($name in $settings.Keys) { $priorEnvironment[$name] = [Environment]::GetEnvironmentVariable($name,'Process'); [Environment]::SetEnvironmentVariable($name,[string]$settings[$name],'Process') }
$started = $false
try {
    Compose @('config','--quiet')
    $started = $true
    Compose @('up','-d','--build','--wait','--wait-timeout','180')
    Compose @('--profile','tools','run','--rm','tools')
    $health = @()
    foreach ($name in @('METRICS_1_PORT','METRICS_2_PORT','METRICS_3_PORT')) {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$($settings[$name])/healthz"
        if ($response.StatusCode -ne 200) { throw "Broker health failed: $name" }
        $health += $response.StatusCode
    }
    $targets = $null
    foreach ($attempt in 1..30) {
        $targets = Invoke-RestMethod -Uri "http://127.0.0.1:$($settings.PROMETHEUS_PORT)/api/v1/targets"
        $active = @($targets.data.activeTargets | Where-Object { $_.labels.job -eq 'streamforge' })
        if ($active.Count -eq 3 -and @($active | Where-Object { $_.health -ne 'up' }).Count -eq 0) { break }
        Start-Sleep -Seconds 2
    }
    if ($active.Count -ne 3 -or @($active | Where-Object { $_.health -ne 'up' }).Count) { throw 'Prometheus did not successfully scrape all three brokers.' }
    $targets | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $evidenceDir 'prometheus-targets.json') -Encoding UTF8
    Compose @('ps','--format','json') | Set-Content -LiteralPath (Join-Path $evidenceDir 'services.jsonl') -Encoding UTF8
    [ordered]@{Passed=$true; Project=$runId; VerifiedAt=[DateTimeOffset]::Now.ToString('o'); BrokerHealth=$health; HealthyPrometheusTargets=$active.Count; DemoPassed=$true} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $evidenceDir 'verification.json') -Encoding UTF8
    Write-Output "DOCKER_COMPOSE_DEMO_PROMETHEUS_PASSED=$evidenceDir"
} finally {
    try {
    if ($started) {
        # These volumes belong only to the unique verification project above.
        if ($runId -notmatch '^streamforge-check-[0-9a-f]{12}$') { throw 'Refusing cleanup of an unexpected Compose project.' }
        Compose @('logs','--no-color') | Set-Content -LiteralPath (Join-Path $evidenceDir 'services.log') -Encoding UTF8
        Compose @('down','--volumes','--remove-orphans')
    }
    } finally {
    foreach ($name in $settings.Keys) { [Environment]::SetEnvironmentVariable($name,$priorEnvironment[$name],'Process') }
    $env:PATH = $priorPath
    # This generated environment file is the only verification credential file.
    Remove-Item -LiteralPath $environmentFile -Force
    }
}
