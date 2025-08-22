
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
    $minuteField = $parts[0]
    $hourField = $parts[1]

    $action = @(
        "docker run --rm",
        "-v ${PWD}\config.yml:/app/config.yml",
        "-v ${PWD}\Настройки.xlsm:/app/Настройки.xlsm",
        "-v ${PWD}\finmodel.db:/app/finmodel.db",
        "-e FINMODEL_SCRIPT=finmodel.scripts.$name",
        "finmodel"
    ) -join ' '

    $args = @("/Create", "/TN", $name, "/TR", $action)

    if ($minuteField -match '^\*/(\d+)$') {
        $interval = [int]$Matches[1]
        $args += @("/SC", "MINUTE", "/MO", $interval, "/ST", "00:00", "/F")
        Write-Host "Created task '$name' every $interval minute(s)"
    } else {
        $minute = [int]$minuteField
        $hour = [int]$hourField
        $time = "{0:D2}:{1:D2}" -f $hour, $minute
        if ($parts.Length -ge 5 -and $parts[4] -ne "*") {
            $weekdayIndex = [int]$parts[4]
            if ($weekdayIndex -eq 7) { $weekdayIndex = 0 }
            $weekdayNames = @("SUN","MON","TUE","WED","THU","FRI","SAT")
            $weekday = $weekdayNames[$weekdayIndex]
            $args += @("/SC", "WEEKLY", "/D", $weekday, "/ST", $time, "/F")
            Write-Host "Created task '$name' weekly on $weekday at $time"
        } else {
            $args += @("/SC", "DAILY", "/ST", $time, "/F")
            Write-Host "Created task '$name' at $time"
        }
    }

    schtasks.exe @args | Out-Null
}
