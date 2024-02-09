
Push-Location $PSScriptRoot

try {
    aws s3 cp "./topic/92ecd19a-d62e-4a03-9488-c5b6a5a86270.json" "s3://async-api-sample--api-bucket/topic/"
}
catch {
    Pop-Location
}