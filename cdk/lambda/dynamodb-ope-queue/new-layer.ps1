
Push-Location $PSScriptRoot
try{
    docker run --rm --name python --entrypoint "bash" -v "$(pwd):/work" public.ecr.aws/lambda/python:3.12 "/work/docker-layer.sh"
}
finally{
    Pop-Location
}
