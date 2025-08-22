#!/usr/bin/env pwsh
<#!
.SYNOPSIS
Registers scheduled tasks to run finmodel.

.DESCRIPTION
Creates daily, weekly, and interval-based tasks using schtasks.exe.
Optional parameters allow customizing times, credentials, and Docker usage.
Use -Cleanup to remove tasks created by this script.
#>

[CmdletBinding()]
param(
    [string]$DailyTime = "09:00",
    [string]$WeeklyTime = "09:00",
    [string]$WeeklyDay = "MON",
    [string]$IntervalStart = "00:00",
    [int]$IntervalMinutes = 120,
    [string]$User,
    [string]$Password,
    [switch]$UseDocker,
    [switch]$Cleanup
)

Set-StrictMode -Version Latest

$logFile = Join-Path $PSScriptRoot "setup_scheduler.log"

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $entry = "[$timestamp] $Message"
    Write-Host $entry
    Add-Content -Path $logFile -Value $entry
}

function Invoke-Schtasks {
    param([string[]]$Arguments)
    Write-Log ("schtasks " + ($Arguments -join ' '))
    $output = schtasks.exe @Arguments 2>&1
    $output | ForEach-Object { Write-Log $_ }
}

$taskNames = @(
    "finmodel-daily",
    "finmodel-weekly",
    "finmodel-multi-daily"
)

if ($Cleanup) {
    foreach ($name in $taskNames) {
        Invoke-Schtasks @("/Delete", "/TN", $name, "/F")
    }
    Write-Log "Tasks removed."
    exit 0
}

$command = if ($UseDocker) { "docker run --rm finmodel finmodel" } else { "finmodel" }
$credArgs = @()
if ($User) {
    $credArgs += @("/RU", $User)
    if ($Password) { $credArgs += @("/RP", $Password) }
}

$dailyArgs = @("/Create", "/SC", "DAILY", "/TN", $taskNames[0], "/ST", $DailyTime, "/TR", "`"$command`"", "/F")
$dailyArgs += $credArgs
Invoke-Schtasks $dailyArgs

$weeklyArgs = @("/Create", "/SC", "WEEKLY", "/D", $WeeklyDay, "/TN", $taskNames[1], "/ST", $WeeklyTime, "/TR", "`"$command`"", "/F")
$weeklyArgs += $credArgs
Invoke-Schtasks $weeklyArgs

$intervalArgs = @("/Create", "/SC", "MINUTE", "/MO", $IntervalMinutes.ToString(), "/TN", $taskNames[2], "/ST", $IntervalStart, "/TR", "`"$command`"", "/F")
$intervalArgs += $credArgs
Invoke-Schtasks $intervalArgs

Write-Log "Scheduler tasks created."
