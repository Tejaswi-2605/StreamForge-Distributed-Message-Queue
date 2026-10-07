param([switch]$Integration)
. "$PSScriptRoot/env.ps1"
New-Item -ItemType Directory -Force artifacts | Out-Null
$env:GOFLAGS = '-buildvcs=false'
if ($Integration) { $env:STREAMFORGE_INTEGRATION='1' } else { $env:STREAMFORGE_INTEGRATION='0' }
go test -json -count=1 ./... > artifacts/tests.jsonl
if ($LASTEXITCODE) { throw 'tests failed' }
go vet ./...
if ($LASTEXITCODE) { throw 'vet failed' }
Write-Output 'NORMAL_TESTS_AND_VET_PASSED'
$compilerTemp = $null
$priorPath = $env:PATH
$priorCompiler = $env:CC
$priorCGO = $env:CGO_ENABLED
try {
    $env:CGO_ENABLED = '1'
    # This optional local archive avoids Windows GCC manifest failures when
    # the workspace path contains spaces. Fresh clones need their own compiler.
    if (!$env:CC -and !(Get-Command gcc -ErrorAction SilentlyContinue) -and (Test-Path -LiteralPath (Join-Path $projectRoot '.tools/gcc.zip'))) {
        $taskTempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        $compilerTemp = Join-Path $taskTempRoot ('streamforge-verify-gcc-' + [guid]::NewGuid().ToString('N'))
        Expand-Archive -LiteralPath (Join-Path $projectRoot '.tools/gcc.zip') -DestinationPath $compilerTemp
        $compilerBin = Join-Path $compilerTemp 'mingw64/bin'
        $env:PATH = "$compilerBin;$env:PATH"
        $env:CC = Join-Path $compilerBin 'gcc.exe'
    }
    go test -race -json -count=1 ./... > artifacts/race.jsonl
    if ($LASTEXITCODE) { throw 'Race detector failed or unavailable; inspect artifacts/race.jsonl and configure a working C compiler.' }
    go mod verify
    if ($LASTEXITCODE) { throw 'Module verification failed' }
    Write-Output 'RACE_TESTS_AND_MODULE_INTEGRITY_PASSED'
} finally {
    $env:PATH = $priorPath
    $env:CC = $priorCompiler
    $env:CGO_ENABLED = $priorCGO
    if ($compilerTemp) {
        $resolvedCompilerTemp = [IO.Path]::GetFullPath($compilerTemp)
        if (!$resolvedCompilerTemp.StartsWith($taskTempRoot.TrimEnd('\') + '\',[StringComparison]::OrdinalIgnoreCase) -or !(Split-Path -Leaf $resolvedCompilerTemp).StartsWith('streamforge-verify-gcc-')) { throw 'Refusing compiler cleanup outside the task temporary directory' }
        if (Test-Path -LiteralPath $resolvedCompilerTemp) { Remove-Item -LiteralPath $resolvedCompilerTemp -Recurse -Force }
    }
}
