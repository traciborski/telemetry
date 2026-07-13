Set-Location -LiteralPath $PSScriptRoot
& podman compose up --detach --build --no-deps service-a service-b service-c service-d
exit $LASTEXITCODE
