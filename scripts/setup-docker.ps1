# Explicit dependency setup. May show a Windows UAC approval dialog; never reboots.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$setupRoot = Split-Path -Parent $PSScriptRoot
$wslResult = Join-Path $setupRoot 'artifacts/wsl-setup.json'
New-Item -ItemType Directory -Force (Join-Path $setupRoot 'artifacts'),(Join-Path $setupRoot '.tools') | Out-Null
$priorPreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try { $version = & wsl.exe --version 2>&1; $wslVersionExit = $LASTEXITCODE } finally { $ErrorActionPreference = $priorPreference }
if ($wslVersionExit) {
    Write-Output 'Windows administrator approval is required for WSL installation.'
    $adminScript = Join-Path $PSScriptRoot 'setup-wsl-admin.ps1'
    $arguments = @('-NoProfile','-ExecutionPolicy','Bypass','-File',('"'+$adminScript+'"'),'-ResultPath',('"'+$wslResult+'"'))
    $cachedInstaller = Join-Path $setupRoot '.tools/wsl.2.7.13.0.x64.msi'
    if (Test-Path -LiteralPath $cachedInstaller) {
        if ((Get-FileHash -LiteralPath $cachedInstaller -Algorithm SHA256).Hash -ne 'a3505a50f4cc585551d11d9de824ba4375448d7a68f2e71d3fb315fa986fc754') { throw 'Cached WSL installer is incomplete or has an unexpected hash.' }
        $arguments += @('-InstallerPath',('"'+$cachedInstaller+'"'))
    }
    $admin = Start-Process -FilePath 'powershell.exe' -Verb RunAs -WindowStyle Hidden -ArgumentList $arguments -PassThru -Wait
    if ($admin.ExitCode -ne 0) { throw 'Windows administrator setup failed; inspect artifacts/wsl-setup.json and wsl-setup.log.' }
}
$dockerRoot = Join-Path $env:LOCALAPPDATA 'Programs/DockerDesktop'
$dockerCLI = Join-Path $dockerRoot 'resources/bin/docker.exe'
$desktop = Join-Path $dockerRoot 'Docker Desktop.exe'
if (!(Test-Path -LiteralPath $dockerCLI)) {
    $installer = Join-Path $setupRoot '.tools/DockerDesktopInstaller.exe'
    if (!(Test-Path -LiteralPath $installer)) {
        Invoke-WebRequest -UseBasicParsing -Uri 'https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe' -OutFile $installer
    }
    $signature = Get-AuthenticodeSignature -LiteralPath $installer
    if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notmatch 'Docker') { throw 'Docker installer publisher signature validation failed.' }
    $install = Start-Process -FilePath $installer -WindowStyle Hidden -ArgumentList @('install','--user','--quiet','--accept-license','--backend=wsl-2') -Wait -PassThru
    if ($install.ExitCode) { throw "Docker installer failed with exit code $($install.ExitCode)." }
}
if (!(Test-Path -LiteralPath $desktop)) { throw 'Docker Desktop executable was not installed at the expected per-user path.' }
Start-Process -FilePath $desktop -WindowStyle Hidden | Out-Null
$ready = $false
foreach ($attempt in 1..60) {
    $priorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $dockerCLI info --format '{{.ServerVersion}}' 2> (Join-Path $setupRoot 'artifacts/docker-startup-error.txt'); $engineExit=$LASTEXITCODE } finally { $ErrorActionPreference=$priorPreference }
    if ($engineExit -eq 0) { $ready=$true; break }
    Start-Sleep -Seconds 2
}
if (!$ready) { throw 'Docker engine did not become ready. Windows may require a restart or Docker Desktop may require an interactive setup action. No restart was performed.' }
Write-Output "DOCKER_READY=$dockerCLI"
