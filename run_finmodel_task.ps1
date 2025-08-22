#!/usr/bin/env pwsh
param(
    [Parameter(Mandatory = $true)]
    [string]$ScriptName
)

Set-StrictMode -Version Latest

docker run --rm \
    -v "${PWD}\config.yml:/app/config.yml" \
    -v "${PWD}\Настройки.xlsm:/app/Настройки.xlsm" \
    -v "${PWD}\finmodel.db:/app/finmodel.db" \
    -e "FINMODEL_SCRIPT=finmodel.scripts.$ScriptName" \
    finmodel

