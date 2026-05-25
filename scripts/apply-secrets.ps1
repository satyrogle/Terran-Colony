param(
    [string]$EnvFile = ".env.staging"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $EnvFile)) {
    throw "Missing $EnvFile. Copy .env.staging.example and fill in rotated credentials."
}

$values = @{}
Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) {
        return
    }

    $key, $value = $line -split "=", 2
    if ($key -and $value) {
        $values[$key.Trim()] = $value.Trim().Trim('"').Trim("'")
    }
}

$databaseUrl = $values["DATABASE_URL"]
if ([string]::IsNullOrWhiteSpace($databaseUrl)) {
    throw "DATABASE_URL is required in $EnvFile."
}

if ($databaseUrl.Contains("URL_ENCODED_PASSWORD") -or $databaseUrl.Contains("<")) {
    throw "DATABASE_URL still contains placeholder values."
}

Write-Host "Injecting secrets into cloudcommander-staging..."
kubectl create secret generic cloudcommander-secrets `
    --from-literal=DATABASE_URL="$databaseUrl" `
    -n cloudcommander-staging `
    --dry-run=client -o yaml | kubectl apply -f -

Write-Host "Secrets applied."
