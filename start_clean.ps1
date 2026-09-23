#requires -Version 5.1

[CmdletBinding()]
param(
    [ValidateSet("ml", "metrics")]
    [string]$Pipeline = "ml",
    [string]$KafkaHome = $env:KAFKA_HOME,
    [switch]$RetrainModel,
    [switch]$KeepStreamingData
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
if (-not (Test-Path -LiteralPath (Join-Path $ProjectRoot "etl_pipeline.py") -PathType Leaf)) {
    throw "Project root is invalid: $ProjectRoot"
}
Set-Location -LiteralPath $ProjectRoot

function Get-EnvOrDefault {
    param([string]$Name, [string]$Default)
    $value = [Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($value)) { return $Default }
    return $value
}

function Resolve-Python {
    $venvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython -PathType Leaf) { return $venvPython }

    $python = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($python) { return $python.Source }

    throw "Python was not found. Run .\setup_windows.ps1 first."
}

function Resolve-KafkaTool {
    param([Parameter(Mandatory)][string]$Name)

    $candidates = @()
    if ($KafkaHome) {
        $candidates += Join-Path $KafkaHome "bin\windows\$Name.bat"
        $candidates += Join-Path $KafkaHome "bin\$Name.bat"
    }

    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate -PathType Leaf) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }

    $command = Get-Command "$Name.bat" -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }

    throw "Cannot find $Name.bat. Set KAFKA_HOME to the extracted Apache Kafka directory."
}

function Test-TcpPort {
    param([string]$HostName = "127.0.0.1", [int]$Port, [int]$TimeoutMs = 800)
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $connect = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $connect.AsyncWaitHandle.WaitOne($TimeoutMs)) { return $false }
        $client.EndConnect($connect)
        return $true
    }
    catch { return $false }
    finally { $client.Dispose() }
}

function Remove-SafeProjectDirectory {
    param([Parameter(Mandatory)][string]$RelativePath)

    $target = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot $RelativePath))
    $rootPrefix = $ProjectRoot.TrimEnd('\') + '\'
    if (-not $target.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove a path outside the project: $target"
    }

    if (Test-Path -LiteralPath $target) {
        Remove-Item -LiteralPath $target -Recurse -Force
        Write-Host "Removed $RelativePath"
    }
}

function Stop-OldProjectProcesses {
    $scriptNames = @(
        "src/spark_processor.py",
        "src\spark_processor.py",
        "src/spark_stream_predictor.py",
        "src\spark_stream_predictor.py",
        "src/producer_kafka.py",
        "src\producer_kafka.py",
        "streamlit run app.py",
        "streamlit run app_ml.py"
    )

    try {
        $processes = Get-CimInstance Win32_Process -ErrorAction Stop |
            Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine }

        foreach ($process in $processes) {
            $matchesProject = $false
            foreach ($name in $scriptNames) {
                if ($process.CommandLine.IndexOf($name, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                    $matchesProject = $true
                    break
                }
            }

            if ($matchesProject) {
                Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
                Write-Host "Stopped old project process PID $($process.ProcessId)"
            }
        }
    }
    catch {
        Write-Warning "Could not inspect old project processes: $($_.Exception.Message)"
    }
}

function Invoke-Python {
    param([Parameter(Mandatory)][string[]]$Arguments)
    & $Python @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed: python $($Arguments -join ' ')"
    }
}

$Python = Resolve-Python
$env:PYSPARK_PYTHON = $Python
$env:PYSPARK_DRIVER_PYTHON = $Python
$env:SPARK_LOCAL_IP = Get-EnvOrDefault "SPARK_LOCAL_IP" "127.0.0.1"
$env:KAFKA_BOOTSTRAP = Get-EnvOrDefault "KAFKA_BOOTSTRAP" "localhost:9092"
$env:KAFKA_TOPIC = Get-EnvOrDefault "KAFKA_TOPIC" "market_data"
$env:KAFKA_STARTING_OFFSETS = Get-EnvOrDefault "KAFKA_STARTING_OFFSETS" "latest"
$env:SPARK_TRIGGER_INTERVAL = Get-EnvOrDefault "SPARK_TRIGGER_INTERVAL" "2 seconds"
$env:STREAMLIT_REFRESH_EVERY = Get-EnvOrDefault "STREAMLIT_REFRESH_EVERY" "1s"
$env:LOOKBACK_DAYS = Get-EnvOrDefault "LOOKBACK_DAYS" "90"
$env:TAIL_ROWS = Get-EnvOrDefault "TAIL_ROWS" "120"
$env:MARKET_CSV_PATH = Get-EnvOrDefault "MARKET_CSV_PATH" "output/market_data_demo.csv"
$env:STREAM_SLEEP_SECONDS = Get-EnvOrDefault "STREAM_SLEEP_SECONDS" "1"
$env:DEMO_SYMBOLS = Get-EnvOrDefault "DEMO_SYMBOLS" "AAPL,MSFT,TSLA"
$env:DEMO_START_DATE = Get-EnvOrDefault "DEMO_START_DATE" "2024-01-01"
$env:DEMO_ROWS_PER_SYMBOL = Get-EnvOrDefault "DEMO_ROWS_PER_SYMBOL" "120"
$env:STREAM_SYMBOL_FILTER = Get-EnvOrDefault "STREAM_SYMBOL_FILTER" $env:DEMO_SYMBOLS

$java = Get-Command java.exe -ErrorAction SilentlyContinue
if (-not $java) {
    throw "Java was not found. Install JDK 17 or 21 and set JAVA_HOME."
}
if (-not $env:JAVA_HOME -or -not (Test-Path -LiteralPath (Join-Path $env:JAVA_HOME "bin\java.exe") -PathType Leaf)) {
    $env:JAVA_HOME = Split-Path -Parent (Split-Path -Parent $java.Source)
}
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

New-Item -ItemType Directory -Force -Path "output", "kafka-runtime-logs" | Out-Null
Stop-OldProjectProcesses

if (-not (Test-TcpPort -Port 9092)) {
    $KafkaStorage = Resolve-KafkaTool "kafka-storage"
    $KafkaServer = Resolve-KafkaTool "kafka-server-start"
    $KafkaConfig = Join-Path $ProjectRoot "kraft.properties"

    $logSetting = Select-String -LiteralPath $KafkaConfig -Pattern '^\s*log\.dirs\s*=' | Select-Object -First 1
    $logDirectory = "kafka-logs"
    if ($logSetting) {
        $logDirectory = ($logSetting.Line -split '=', 2)[1].Trim()
    }
    if (-not [System.IO.Path]::IsPathRooted($logDirectory)) {
        $logDirectory = Join-Path $ProjectRoot $logDirectory
    }
    New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null

    $metadataFile = Join-Path $logDirectory "meta.properties"
    if (-not (Test-Path -LiteralPath $metadataFile -PathType Leaf)) {
        Write-Host "Formatting Kafka storage..."
        $clusterId = (& $KafkaStorage random-uuid | Select-Object -Last 1).Trim()
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($clusterId)) {
            throw "Could not create a Kafka cluster id."
        }
        & $KafkaStorage format -t $clusterId -c $KafkaConfig
        if ($LASTEXITCODE -ne 0) { throw "Could not format Kafka storage." }
    }

    Write-Host "Starting Kafka..."
    $kafkaCommandLine = '""{0}" "{1}""' -f $KafkaServer, $KafkaConfig
    $KafkaProcess = Start-Process -FilePath $env:ComSpec `
        -ArgumentList @("/d", "/s", "/c", $kafkaCommandLine) `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput (Join-Path $ProjectRoot "kafka-runtime-logs\kafka.out.log") `
        -RedirectStandardError (Join-Path $ProjectRoot "kafka-runtime-logs\kafka.err.log") `
        -WindowStyle Hidden `
        -PassThru

    $deadline = [DateTime]::UtcNow.AddSeconds(40)
    while ([DateTime]::UtcNow -lt $deadline -and -not (Test-TcpPort -Port 9092)) {
        Start-Sleep -Seconds 1
    }
    if (-not (Test-TcpPort -Port 9092)) {
        throw "Kafka did not open port 9092. Check kafka-runtime-logs\kafka.err.log."
    }
    Write-Host "Kafka started (PID $($KafkaProcess.Id))."
}
else {
    Write-Host "Kafka is already listening on localhost:9092."
}

if (-not $KeepStreamingData) {
    Remove-SafeProjectDirectory "output\parquet\bronze_market_data"
    if ($Pipeline -eq "ml") {
        Remove-SafeProjectDirectory "output\parquet\market_predictions"
        Remove-SafeProjectDirectory "checkpoints\spark_market_predictions"
    }
    else {
        Remove-SafeProjectDirectory "output\parquet\market_metrics"
        Remove-SafeProjectDirectory "checkpoints\spark_market_metrics"
    }
}

Write-Host "Running ETL..."
Invoke-Python @("etl_pipeline.py")

Write-Host "Creating demo data..."
Invoke-Python @("create_demo_data.py")

if ($Pipeline -eq "ml") {
    if ($RetrainModel -or -not (Test-Path -LiteralPath "models\stock_direction_rf")) {
        Write-Host "Building features and training the Spark ML model..."
        Invoke-Python @("mapreduce_analysis.py")
        Invoke-Python @("src\spark_ml_train.py")
    }
    $sparkScript = "src\spark_stream_predictor.py"
    $dashboardScript = "app_ml.py"
}
else {
    $sparkScript = "src\spark_processor.py"
    $dashboardScript = "app.py"
}

Write-Host "Starting Spark ($Pipeline pipeline)..."
$SparkProcess = Start-Process -FilePath $Python `
    -ArgumentList @($sparkScript) `
    -WorkingDirectory $ProjectRoot `
    -RedirectStandardOutput (Join-Path $ProjectRoot "kafka-runtime-logs\spark.out.log") `
    -RedirectStandardError (Join-Path $ProjectRoot "kafka-runtime-logs\spark.err.log") `
    -WindowStyle Hidden `
    -PassThru

Start-Sleep -Seconds 12
if ($SparkProcess.HasExited) {
    throw "Spark exited early. Check kafka-runtime-logs\spark.err.log."
}

Write-Host "Starting Streamlit dashboard..."
$DashboardProcess = Start-Process -FilePath $Python `
    -ArgumentList @("-m", "streamlit", "run", $dashboardScript, "--server.port", "8501") `
    -WorkingDirectory $ProjectRoot `
    -RedirectStandardOutput (Join-Path $ProjectRoot "kafka-runtime-logs\streamlit.out.log") `
    -RedirectStandardError (Join-Path $ProjectRoot "kafka-runtime-logs\streamlit.err.log") `
    -WindowStyle Hidden `
    -PassThru

Start-Sleep -Seconds 5
if ($DashboardProcess.HasExited) {
    throw "Streamlit exited early. Check kafka-runtime-logs\streamlit.err.log."
}

Write-Host "Starting Kafka producer in this terminal..."
Invoke-Python @("src\producer_kafka.py")

Write-Host ""
Write-Host "Producer finished."
Write-Host "Dashboard: http://localhost:8501"
Write-Host "Spark PID: $($SparkProcess.Id)"
Write-Host "Streamlit PID: $($DashboardProcess.Id)"
Write-Host "Stop them with: Stop-Process -Id $($SparkProcess.Id),$($DashboardProcess.Id)"
