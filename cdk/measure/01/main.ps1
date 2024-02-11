
try {
    Push-Location $PSScriptRoot

    Write-Host "********** init ************"
    ## remove
    . .\rm-response.ps1
    . .\rm-topic.ps1

    Write-Host "********** upload ************"

    . .\upload-10-topic.ps1

    
    Write-Host "********** check reponse ************"

    $m = Measure-Command {

        foreach ($i in (1..600)) {
            
            Start-Sleep -Seconds 1

            $r = aws s3 ls "s3://async-api-sample--response-bucket/response/" --summarize
            $r_count = $($($r | Select-String "Total Objects") -match "\d+" | Out-Null; [int]($Matches[0]))

            Write-Host "check $i : count $r_count"
            Write-Host $r

            if ($r_count -ge 10) {
                break
            }
        }
    }

    Write-Host "********** result time ************"
    Write-Host $m
    $m | New-Item -ItemType File -Path "./out/result_$($(Get-Date).ToString('yyyy-MM-dd_HH-mm-ss')).txt" -Force | Out-Null

}
catch {
    Pop-Location
}

