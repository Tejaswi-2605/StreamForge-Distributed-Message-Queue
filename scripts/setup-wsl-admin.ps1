param([string]$ResultPath, [string]$InstallerPath)
$ErrorActionPreference = 'Stop'
$setupRoot = Split-Path -Parent $PSScriptRoot
if (!$ResultPath) { $ResultPath = Join-Path $setupRoot 'artifacts/wsl-setup.json' }
New-Item -ItemType Directory -Force (Join-Path $setupRoot 'artifacts') | Out-Null
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (!$principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'This helper requires Windows administrator approval. Run scripts/setup-docker.ps1.'
}
$logPath = Join-Path $setupRoot 'artifacts/wsl-setup.log'
$result = [ordered]@{Installed=$false; ExitCode=-1; Error=''; RestartMayBeRequired=$false}
try {
    if ($InstallerPath) {
        $signature = Get-AuthenticodeSignature -LiteralPath $InstallerPath
        if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notmatch 'Microsoft') { throw 'WSL installer signature validation failed.' }
        $install = Start-Process -FilePath 'msiexec.exe' -WindowStyle Hidden -ArgumentList @('/i',('"'+$InstallerPath+'"'),'/qn','/norestart','/L*v',('"'+$logPath+'"')) -Wait -PassThru
        $installerCode = $install.ExitCode
        if ($installerCode -ne 0 -and $installerCode -ne 3010) { throw "WSL MSI installation failed: $installerCode; see $logPath" }
    } else {
        & winget install --id Microsoft.WSL --exact --source winget --silent --accept-source-agreements --accept-package-agreements --disable-interactivity *> $logPath
        $installerCode = $LASTEXITCODE
    }
    # Winget can report already-installed/no applicable update as a nonzero
    # result. The installed program's version is the authoritative check.
    $priorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { $version = & wsl.exe --version 2>&1; $versionExit=$LASTEXITCODE } finally { $ErrorActionPreference=$priorPreference }
    if ($versionExit) { throw "WSL installation failed with exit code $installerCode; see $logPath" }
    & wsl.exe --install --no-distribution >> $logPath 2>&1
    $featureCode = $LASTEXITCODE
    if ($featureCode -ne 0 -and $featureCode -ne 3010) {
        throw "WSL feature setup failed with exit code $featureCode; see $logPath"
    }
    $result.Installed = $true
    $result.ExitCode = 0
    $result.RestartMayBeRequired = $true
    $result.VersionOutput = (($version -join "`n") -replace [char]0,'')
} catch {
    $result.Error = $_.Exception.Message
} finally {
    $result | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $ResultPath -Encoding UTF8
}
if (!$result.Installed) { exit 1 }
