. "$PSScriptRoot/env.ps1"
New-Item -ItemType Directory -Force bin | Out-Null
foreach ($name in @('broker','admin','producer','consumer','benchmark','replay','demo')) {
 go build -buildvcs=false -trimpath -o "bin/streamforge-$name.exe" "./cmd/$name"
 if ($LASTEXITCODE) { throw "build failed: $name" }
}
