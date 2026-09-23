#requires -Version 5.1

[CmdletBinding()]
param(
    [string]$KafkaHome = $env:KAFKA_HOME,
    [string]$BootstrapServer = "localhost:9092",
    [string]$Topic = "market_data",
    [int]$Partition = 0,
    [switch]$FromBeginning
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($KafkaHome)) {
    throw "KAFKA_HOME is not set. Pass -KafkaHome 'C:\path\to\kafka'."
}

$KafkaHome = [System.IO.Path]::GetFullPath($KafkaHome)
$consumer = Join-Path $KafkaHome "bin\windows\kafka-console-consumer.bat"
if (-not (Test-Path -LiteralPath $consumer -PathType Leaf)) {
    throw "Cannot find Kafka consumer: $consumer"
}

$env:LOG_DIR = Join-Path $PSScriptRoot "kafka-runtime-logs\tools"
New-Item -ItemType Directory -Force -Path $env:LOG_DIR | Out-Null

$logConfig = Join-Path $KafkaHome "config\tools-log4j2.yaml"
if (Test-Path -LiteralPath $logConfig -PathType Leaf) {
    $logUri = ([System.Uri][System.IO.Path]::GetFullPath($logConfig)).AbsoluteUri
    $env:KAFKA_LOG4J_OPTS = "-Dlog4j2.configurationFile=$logUri"
}

$arguments = @(
    "--bootstrap-server", $BootstrapServer,
    "--topic", $Topic,
    "--partition", $Partition,
    "--property", "print.timestamp=true",
    "--property", "print.partition=true",
    "--property", "print.offset=true",
    "--property", "print.key=true",
    "--property", "key.separator= | "
)

if ($FromBeginning) {
    $arguments += "--from-beginning"
}

Write-Host "Watching Kafka topic '$Topic' on $BootstrapServer"
Write-Host "Press Ctrl+C to stop."
& $consumer @arguments
exit $LASTEXITCODE
