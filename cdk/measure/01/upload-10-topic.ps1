function Invoke-Parallel {
    1..10 | ForEach-Object -Parallel {
        $filename = "$(New-Guid).json"
        Write-Output '{}' | aws s3 cp - "s3://async-api-sample--api-bucket/topic/$filename"
        Write-Host "$_ : $filename"
    } -ThrottleLimit 10
}
Invoke-Parallel
