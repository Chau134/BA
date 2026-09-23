#requires -Version 5.1

[CmdletBinding()]
param(
    [string]$KafkaHome = $env:KAFKA_HOME,
    [switch]$SkipPackageInstall
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
Set-Location -LiteralPath $ProjectRoot

function Resolve-PythonLauncher {
    $py = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($py) {
        return ,@($py.Source, "-3")
    }

    $python = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($python) {
        return ,@($python.Source)
    }

    throw "Python 3 was not found. Install it from python.org, then reopen PowerShell."
}

function Resolve-JavaHome {
    if ($env:JAVA_HOME) {
        $javaFromHome = Join-Path $env:JAVA_HOME "bin\java.exe"
        if (Test-Path -LiteralPath $javaFromHome -PathType Leaf) {
            return [System.IO.Path]::GetFullPath($env:JAVA_HOME)
        }
    }

    $java = Get-Command java.exe -ErrorAction SilentlyContinue
    if (-not $java) {
        throw "Java was not found. Install a 64-bit JDK 17 or 21, set JAVA_HOME, and reopen PowerShell."
    }

    return Split-Path -Parent (Split-Path -Parent $java.Source)
}

Write-Host "Preparing Windows environment in $ProjectRoot"

$venvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    $launcher = Resolve-PythonLauncher
    $launcherExe = $launcher[0]
    $launcherArgs = @()
    if ($launcher.Count -gt 1) {
        $launcherArgs += $launcher[1..($launcher.Count - 1)]
    }
    $launcherArgs += @("-m", "venv", ".venv")

    Write-Host "Creating .venv..."
    & $launcherExe @launcherArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the virtual environment."
    }
}

if (-not $SkipPackageInstall) {
    Write-Host "Installing Python packages..."
    & $venvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw "Could not upgrade pip." }

    & $venvPython -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw "Could not install requirements.txt." }
}

$resolvedJavaHome = Resolve-JavaHome
$env:JAVA_HOME = $resolvedJavaHome
$env:Path = "$resolvedJavaHome\bin;$env:Path"
$env:PYSPARK_PYTHON = $venvPython
$env:PYSPARK_DRIVER_PYTHON = $venvPython
$env:SPARK_LOCAL_IP = "127.0.0.1"

Write-Host ""
Write-Host "Environment check"
Write-Host "  Python:    $venvPython"
& $venvPython --version
Write-Host "  JAVA_HOME: $resolvedJavaHome"
& (Join-Path $resolvedJavaHome "bin\java.exe") -version

if ($KafkaHome) {
    $KafkaHome = [System.IO.Path]::GetFullPath($KafkaHome)
    $kafkaServer = Join-Path $KafkaHome "bin\windows\kafka-server-start.bat"
    if (Test-Path -LiteralPath $kafkaServer -PathType Leaf) {
        Write-Host "  KAFKA_HOME: $KafkaHome"
    }
    else {
        Write-Warning "KAFKA_HOME does not contain bin\windows\kafka-server-start.bat: $KafkaHome"
    }
}
else {
    Write-Warning "KAFKA_HOME is not set. Extract the Apache Kafka binary package, then set it, for example:"
    Write-Host '  [Environment]::SetEnvironmentVariable("KAFKA_HOME", "C:\tools\kafka", "User")'
}

Write-Host ""
Write-Host "Setup completed. Activate this environment with:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "Run the demo with:"
Write-Host "  .\start_clean.ps1"
