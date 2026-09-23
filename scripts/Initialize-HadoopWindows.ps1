#requires -Version 5.1

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$ProjectRoot
)

$ErrorActionPreference = "Stop"

$resolvedProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$sourcePath = Join-Path $resolvedProjectRoot "scripts\winutils.cs"
$hadoopHome = Join-Path $resolvedProjectRoot ".runtime\hadoop"
$binDirectory = Join-Path $hadoopHome "bin"
$winutilsPath = Join-Path $binDirectory "winutils.exe"

if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
    throw "Missing Windows Hadoop helper source: $sourcePath"
}
New-Item -ItemType Directory -Force -Path $binDirectory | Out-Null

if (-not (Test-Path -LiteralPath $winutilsPath -PathType Leaf)) {
    Write-Host "Building local Hadoop Windows helper..."
    $source = Get-Content -LiteralPath $sourcePath -Raw
    Add-Type -TypeDefinition $source `
        -Language CSharp `
        -OutputAssembly $winutilsPath `
        -OutputType ConsoleApplication
}

if (-not (Test-Path -LiteralPath $winutilsPath -PathType Leaf)) {
    throw "Could not build the local Hadoop Windows helper: $winutilsPath"
}

Write-Output $hadoopHome
