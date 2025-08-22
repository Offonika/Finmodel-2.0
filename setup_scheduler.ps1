
#!/usr/bin/env pwsh
# Requires module 'powershell-yaml' for ConvertFrom-Yaml.
Set-StrictMode -Version Latest


param(
    [string]$SchedulePath = "schedule.yml"
)
Set-StrictMode -Version Latest

if (-not (Get-Command ConvertFrom-Yaml -ErrorAction SilentlyContinue)) {
    try {
        Import-Module powershell-yaml -ErrorAction Stop
    } catch {
        Write-Error "ConvertFrom-Yaml not found. Install the 'powershell-yaml' module: Install-Module powershell-yaml"
        exit 1
    }
}

if (-not (Test-Path $SchedulePath)) {
    Write-Error "Schedule file '$SchedulePath' not found. Copy schedule.example.yml to schedule.yml and edit."
    exit 1
}

$schedule = Get-Content $SchedulePath -Raw | ConvertFrom-Yaml

foreach ($item in $schedule.GetEnumerator()) {
    $name = $item.Key
    $cron = $item.Value
    $parts = $cron -split ' '
    if ($parts.Length -lt 2) {
        Write-Warning "Skipping '$name': invalid cron '$cron'"
        continue
    }
    $minute = [int]$parts[0]
    $hour = [int]$parts[1]
    $time = "{0:D2}:{1:D2}" -f $hour, $minute

    $action = @(
        "docker run --rm",
        "-v ${PWD}\config.yml:/app/config.yml",
        "-v ${PWD}\Настройки.xlsm:/app/Настройки.xlsm",
        "-v ${PWD}\finmodel.db:/app/finmodel.db",
        "-e FINMODEL_SCRIPT=finmodel.scripts.$name",
        "finmodel"
    ) -join ' '

    schtasks.exe /Create /TN $name /TR $action /SC DAILY /ST $time /F | Out-Null
    Write-Host "Created task '$name' at $time"
}
