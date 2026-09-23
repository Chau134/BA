# Big Data & Business Intelligence Project

## Real-time Market Risk Classification with MapReduce, Kafka, Spark Streaming and Spark MLlib

Dự án xây dựng một pipeline Big Data và Business Intelligence để phân tích dữ liệu thị trường gồm cổ phiếu, dầu và vàng. Hệ thống hỗ trợ cả xử lý batch/offline và streaming realtime.

Trọng tâm hiện tại của dự án là:

```text
Classify current market risk state:
- HIGH RISK
- NORMAL
```

Dự án không còn tập trung vào việc dự đoán giá cổ phiếu tăng/giảm trong tương lai, vì bài toán đó có độ nhiễu cao và khó vượt baseline ổn định. Thay vào đó, hệ thống phân loại **trạng thái rủi ro hiện tại của thị trường** dựa trên các đặc trưng như return, volatility, moving average, correlation và alert level.

---

# Chạy nhanh trên máy Windows hiện tại

Máy hiện tại đã có:

* Python 3.12.
* Java JDK 21.
* Apache Kafka 4.1.2 tại `C:\A\DOWN\kafka_2.13-4.1.2`.

Không cần tải thêm Java hoặc Kafka. Phần còn thiếu là các thư viện Python trong `requirements.txt`, bao gồm `pyspark`, `yfinance`, `kafka-python-ng`, `streamlit` và `pyarrow`.

## Cài đặt lần đầu

Mở PowerShell và chạy nguyên khối lệnh sau:

```powershell
Set-Location 'C:\A\BA_BTL'
Set-ExecutionPolicy -Scope Process Bypass

$KafkaDir = 'C:\A\DOWN\kafka_2.13-4.1.2'

if (-not (Test-Path -LiteralPath "$KafkaDir\bin\windows\kafka-server-start.bat")) {
    throw "Không tìm thấy Kafka tại $KafkaDir"
}

# Lưu KAFKA_HOME cho các cửa sổ PowerShell mở sau này.
[Environment]::SetEnvironmentVariable('KAFKA_HOME', $KafkaDir, 'User')

# Dùng ngay trong cửa sổ PowerShell hiện tại.
$env:KAFKA_HOME = $KafkaDir

# Tạo .venv và cài toàn bộ thư viện Python từ requirements.txt.
.\setup_windows.ps1 -KafkaHome $KafkaDir
```

Lệnh `setup_windows.ps1` sẽ tự động chạy tương đương với:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Quá trình cài Python packages cần kết nối Internet và chỉ cần thực hiện một lần.

## Kiểm tra sau khi cài

```powershell
Set-Location 'C:\A\BA_BTL'

.\.venv\Scripts\python.exe -c "import pyspark, yfinance, kafka, streamlit, pyarrow; print('PySpark:', pyspark.__version__); print('yfinance:', yfinance.__version__); print('Python packages: OK')"

java -version

& 'C:\A\DOWN\kafka_2.13-4.1.2\bin\windows\kafka-topics.bat' --version
```

Kết quả mong đợi:

* Python packages in ra `Python packages: OK`.
* Java báo phiên bản 21.
* Kafka báo phiên bản `4.1.2`.

Kafka có thể in thêm cảnh báo Log4j khi kiểm tra phiên bản. Nếu lệnh vẫn trả về `4.1.2` thì Kafka CLI đã hoạt động.

## Chạy toàn bộ hệ thống

Sau khi cài xong, vẫn trong PowerShell tại thư mục dự án:

```powershell
Set-Location 'C:\A\BA_BTL'
Set-ExecutionPolicy -Scope Process Bypass

$KafkaDir = 'C:\A\DOWN\kafka_2.13-4.1.2'
.\start_clean.ps1 -KafkaHome $KafkaDir
```

Script sẽ tự thực hiện theo thứ tự:

1. Khởi động Kafka KRaft nếu cổng `9092` chưa chạy.
2. Dọn output/checkpoint streaming cũ.
3. Chạy ETL và tạo dữ liệu demo.
4. Khởi động Spark Streaming Predictor.
5. Khởi động Streamlit.
6. Chạy Kafka Producer.

Dashboard:

```text
http://localhost:8501
```

Lần đầu Spark chạy có thể mất thêm thời gian để tải Kafka connector từ Maven. Windows Firewall có thể hỏi quyền truy cập cho Java; cho phép trên mạng riêng.

## Những lần chạy sau

Không cần chạy lại `setup_windows.ps1`. Chỉ chạy:

```powershell
Set-Location 'C:\A\BA_BTL'
Set-ExecutionPolicy -Scope Process Bypass
.\start_clean.ps1 -KafkaHome 'C:\A\DOWN\kafka_2.13-4.1.2'
```

Muốn giữ output/checkpoint streaming cũ:

```powershell
.\start_clean.ps1 `
    -KafkaHome 'C:\A\DOWN\kafka_2.13-4.1.2' `
    -KeepStreamingData
```

Muốn train lại model trước khi chạy:

```powershell
.\start_clean.ps1 `
    -KafkaHome 'C:\A\DOWN\kafka_2.13-4.1.2' `
    -RetrainModel
```

---

# 1. Mục tiêu dự án

Dự án mô phỏng một hệ thống Big Data + BI hoàn chỉnh, gồm:

* Thu thập dữ liệu tài chính lịch sử từ Yahoo Finance.
* Xử lý ETL dữ liệu stock, oil và gold.
* Tạo feature bằng tư duy MapReduce.
* Train mô hình Machine Learning bằng Spark MLlib.
* Mô phỏng realtime data stream bằng Apache Kafka.
* Xử lý stream bằng Spark Structured Streaming.
* Phân loại rủi ro thị trường realtime bằng mô hình ML đã train.
* Hiển thị kết quả trên Streamlit dashboard.

Bài toán ML chính:

```text
Current Market Risk Classification

Input:
    stock/oil/gold price, return, volatility, moving average, correlation

Output:
    HIGH RISK hoặc NORMAL
```

---

# 2. Kiến trúc tổng quan

```text
Yahoo Finance
    │
    ▼
generate_sample_data.py
    │
    ├── data/stock_data.csv
    ├── data/oil_price_raw.json
    └── data/gold_price_raw.json
    │
    ▼
etl_pipeline.py
    │
    ▼
output/market_data_final.csv
    │
    ├──────────────────────────────┐
    │                              │
    ▼                              ▼
mapreduce_analysis.py        create_demo_data.py
    │                              │
    ▼                              ▼
output/market_analysis_results.csv output/market_data_demo.csv
    │                              │
    ▼                              ▼
src/spark_ml_train.py        src/producer_kafka.py
    │                              │
    ▼                              ▼
models/stock_direction_rf    Kafka topic: market_data
                                   │
                                   ▼
                         src/spark_stream_predictor.py
                                   │
                                   ▼
                      output/parquet/market_predictions
                                   │
                                   ▼
                              app_ml.py
```

---

# 3. Công nghệ sử dụng

| Nhóm                       | Công nghệ                                   |
| -------------------------- | ------------------------------------------- |
| Language                   | Python                                      |
| Data Collection            | yfinance / Yahoo Finance                    |
| Batch Processing           | pandas                                      |
| MapReduce-style Processing | pandas groupby, rolling, reduce aggregation |
| Streaming Broker           | Apache Kafka                                |
| Big Data Processing        | Apache Spark Structured Streaming           |
| Machine Learning           | Spark MLlib                                 |
| Model                      | RandomForestClassifier                      |
| Storage                    | CSV, JSON, Parquet                          |
| Dashboard                  | Streamlit, Plotly                           |
| Runtime                    | Java 17 hoặc 21 trên Windows                |

---

# 4. Cấu trúc thư mục

```text
BA_BTL/
├── README.md
├── app.py
├── app_ml.py
├── create_demo_data.py
├── etl_pipeline.py
├── generate_sample_data.py
├── mapreduce_analysis.py
├── preview_data.py
├── requirements.txt
├── kraft.properties
├── setup_windows.ps1
├── start_clean.ps1
├── start_clean.sh            # script cũ dành cho macOS/Linux
│
├── src/
│   ├── producer_kafka.py
│   ├── spark_processor.py
│   ├── spark_ml_train.py
│   └── spark_stream_predictor.py
│
├── data/
│   ├── stock_data.csv
│   ├── oil_price_raw.json
│   └── gold_price_raw.json
│
├── output/
│   ├── market_data_final.csv
│   ├── market_data_demo.csv
│   ├── market_analysis_results.csv
│   ├── market_analysis_results_weekly.csv
│   ├── market_analysis_results_monthly_volatility.csv
│   ├── ml_evaluation.csv
│   ├── ml_feature_importance.csv
│   ├── ml_predictions_sample.csv
│   └── parquet/
│       ├── bronze_market_data/
│       └── market_predictions/
│
├── models/
│   ├── stock_direction_rf/
│   └── stock_direction_rf_meta.json
│
├── checkpoints/
│   └── spark_market_predictions/
│
├── kafka-logs/
└── kafka-runtime-logs/
```

---

# 5. Vai trò từng file

## 5.1 `generate_sample_data.py`

File này dùng để tải dữ liệu lịch sử từ Yahoo Finance.

Output:

```text
data/stock_data.csv
data/oil_price_raw.json
data/gold_price_raw.json
```

Các mã stock/index mặc định có thể gồm:

```text
AAPL, MSFT, TSLA, XOM, JPM, WMT, ^DJI, ^GSPC, ^IXIC, ^RUT
```

Có thể thay đổi danh sách mã bằng biến môi trường:

```powershell
$env:STOCK_SYMBOLS = 'AAPL,MSFT,TSLA'
python .\generate_sample_data.py
```

---

## 5.2 `etl_pipeline.py`

File này thực hiện ETL:

1. Đọc dữ liệu stock.
2. Đọc dữ liệu oil.
3. Đọc dữ liệu gold.
4. Chuẩn hóa kiểu dữ liệu.
5. Join dữ liệu theo ngày.
6. Xuất file tổng hợp.

Output:

```text
output/market_data_final.csv
```

Schema chính:

```text
date
symbol
stock_close
oil_price
gold_price
```

---

## 5.3 `preview_data.py`

Dùng để kiểm tra nhanh dữ liệu sau ETL.

Chạy:

```powershell
python preview_data.py
```

Thông tin kiểm tra gồm:

* Tổng số dòng.
* Danh sách cột.
* Danh sách symbol.
* Date range.
* Số dòng theo symbol.
* Một số dòng đầu/cuối.

---

## 5.4 `mapreduce_analysis.py`

Đây là file quan trọng trong phần MapReduce-style feature engineering.

Input:

```text
output/market_data_final.csv
```

Output:

```text
output/market_analysis_results.csv
output/market_analysis_results_weekly.csv
output/market_analysis_results_monthly_volatility.csv
```

Vai trò:

```text
MAP phase:
    - Tính daily return.
    - Tạo lag features.
    - Gắn week/month/year.

REDUCE phase:
    - Tính moving average.
    - Tính volatility.
    - Tính rolling correlation.
    - Tính alert level.
    - Tạo ML label target_market_risk.
```

File này tạo label:

```text
target_market_risk
```

Trong đó:

```text
1 = HIGH RISK
0 = NORMAL
```

---

## 5.5 `src/spark_ml_train.py`

File này train mô hình Machine Learning bằng Spark MLlib.

Input:

```text
output/market_analysis_results.csv
```

Output:

```text
models/stock_direction_rf
models/stock_direction_rf_meta.json
output/ml_evaluation.csv
output/ml_feature_importance.csv
output/ml_predictions_sample.csv
```

Thuật toán sử dụng:

```text
RandomForestClassifier
```

Pipeline ML:

```text
VectorAssembler
    ↓
RandomForestClassifier
    ↓
PipelineModel
```

---

## 5.6 `create_demo_data.py`

File này tạo dữ liệu demo nhỏ để producer stream lên Kafka.

Input:

```text
output/market_data_final.csv
```

Output:

```text
output/market_data_demo.csv
```

Mặc định file demo lấy:

```text
AAPL, MSFT, TSLA
120 dòng mới nhất mỗi symbol
```

Chạy mặc định:

```powershell
python create_demo_data.py
```

Chạy tùy chỉnh:

```powershell
$env:DEMO_SYMBOLS = 'AAPL,MSFT,TSLA'
$env:DEMO_ROWS_PER_SYMBOL = '120'
$env:DEMO_START_DATE = '2024-01-01'
python .\create_demo_data.py
```

Nếu muốn demo 1 symbol:

```powershell
$env:DEMO_SYMBOLS = 'TSLA'
$env:DEMO_ROWS_PER_SYMBOL = '120'
$env:DEMO_START_DATE = '2024-01-01'
python .\create_demo_data.py
```

---

## 5.7 `src/producer_kafka.py`

File này mô phỏng realtime streaming.

Input mặc định:

```text
output/market_data_final.csv
```

Khi demo nên dùng:

```text
output/market_data_demo.csv
```

Producer đọc từng dòng CSV, convert thành JSON và gửi vào Kafka topic:

```text
market_data
```

Chạy:

```powershell
$env:MARKET_CSV_PATH = 'output/market_data_demo.csv'
$env:STREAM_SLEEP_SECONDS = '1'
python .\src\producer_kafka.py
```

Nếu `STREAM_SLEEP_SECONDS=1`, producer gửi 1 record mỗi giây.

Nếu demo nhiều symbol, ví dụ `AAPL, MSFT, TSLA`, producer sẽ gửi lần lượt:

```text
AAPL → MSFT → TSLA → AAPL → MSFT → TSLA
```

---

## 5.8 `src/spark_stream_predictor.py`

File này chạy Spark Structured Streaming và realtime ML inference.

Input:

```text
Kafka topic: market_data
```

Output:

```text
output/parquet/bronze_market_data
output/parquet/market_predictions
checkpoints/spark_market_predictions
```

Flow:

```text
Kafka message
    ↓
Spark parse JSON
    ↓
Tính return, moving average, volatility, correlation
    ↓
Load RandomForest model
    ↓
Predict HIGH RISK / NORMAL
    ↓
Write Parquet
```

Chạy ví dụ:

```powershell
$env:STREAM_SYMBOL_FILTER = 'AAPL,MSFT,TSLA'
$env:KAFKA_TOPIC = 'market_data'
$env:KAFKA_STARTING_OFFSETS = 'latest'
$env:SPARK_TRIGGER_INTERVAL = '2 seconds'
$env:ROLLING_WINDOW = '10'
python .\src\spark_stream_predictor.py
```

Nếu không muốn filter symbol:

```powershell
Remove-Item Env:STREAM_SYMBOL_FILTER -ErrorAction SilentlyContinue
$env:KAFKA_TOPIC = 'market_data'
$env:KAFKA_STARTING_OFFSETS = 'latest'
$env:SPARK_TRIGGER_INTERVAL = '2 seconds'
$env:ROLLING_WINDOW = '10'
python .\src\spark_stream_predictor.py
```

---

## 5.9 `app_ml.py`

Dashboard chính cho pipeline ML realtime.

Dashboard đọc:

```text
output/parquet/market_predictions
```

Hiển thị:

* Giá stock.
* Giá oil.
* Giá gold.
* Probability HIGH RISK.
* Prediction label: HIGH RISK / NORMAL.
* Rolling correlation.
* Daily return.
* Model evaluation.
* Feature importance.
* Latest prediction table.

Dashboard có dropdown chọn symbol. Dropdown tự lấy danh sách symbol từ `output/parquet/market_predictions`.

Chạy:

```powershell
python -m streamlit run app_ml.py
```

Hoặc cấu hình thêm:

```powershell
$env:DEFAULT_SYMBOL = 'TSLA'
$env:STREAMLIT_REFRESH_EVERY = '1s'
$env:LOOKBACK_DAYS = '90'
$env:TAIL_ROWS = '120'
python -m streamlit run .\app_ml.py
```

---

# 6. Market Risk là gì trong dự án này?

Trong dự án này, **market risk** là trạng thái rủi ro hiện tại của thị trường đối với từng symbol.

Hệ thống phân loại:

```text
HIGH RISK
NORMAL
```

Market risk không phải là dự đoán giá tương lai. Đây là bài toán phân loại trạng thái hiện tại dựa trên các feature thị trường.

Ví dụ:

```text
Nếu stock biến động mạnh,
hoặc oil/gold biến động mạnh,
hoặc volatility cao,
hoặc correlation giữa stock và commodity mạnh,
thì trạng thái có thể là HIGH RISK.
```

---

# 7. Cách tính `target_market_risk`

Label dùng để train model là:

```text
target_market_risk
```

Trong đó:

```text
target_market_risk = 1 → HIGH RISK
target_market_risk = 0 → NORMAL
```

Một dòng dữ liệu được gắn nhãn HIGH RISK nếu thỏa ít nhất một điều kiện:

```text
abs(stock_return_pct) >= STOCK_RETURN_RISK_PCT
hoặc abs(oil_return_pct) >= COMMODITY_RETURN_RISK_PCT
hoặc abs(gold_return_pct) >= COMMODITY_RETURN_RISK_PCT
hoặc stock_volatility thuộc nhóm cao
hoặc oil_volatility thuộc nhóm cao
hoặc gold_volatility thuộc nhóm cao
hoặc alert_level == HIGH
```

Mặc định:

```text
STOCK_RETURN_RISK_PCT = 2.0
COMMODITY_RETURN_RISK_PCT = 2.0
VOLATILITY_QUANTILE = 0.75
ROLLING_WINDOW = 10
```

Nghĩa là:

```text
Nếu stock/oil/gold biến động từ 2% trở lên,
hoặc volatility thuộc nhóm 25% cao nhất,
hoặc alert_level là HIGH,
thì label là HIGH RISK.
```

---

# 8. Các feature chính

## 8.1 Daily return

```text
stock_return_pct = phần trăm thay đổi stock_close so với ngày trước
oil_return_pct   = phần trăm thay đổi oil_price so với ngày trước
gold_return_pct  = phần trăm thay đổi gold_price so với ngày trước
```

Công thức:

```text
return_pct = (price_today - price_yesterday) / price_yesterday * 100
```

---

## 8.2 Moving average

```text
stock_ma = trung bình stock_close trong rolling window
oil_ma   = trung bình oil_price trong rolling window
gold_ma  = trung bình gold_price trong rolling window
```

Mặc định:

```text
ROLLING_WINDOW = 10
```

---

## 8.3 Volatility

Volatility là độ lệch chuẩn của return trong rolling window:

```text
stock_volatility = std(stock_return_pct trong 10 ngày gần nhất)
oil_volatility   = std(oil_return_pct trong 10 ngày gần nhất)
gold_volatility  = std(gold_return_pct trong 10 ngày gần nhất)
```

Volatility càng cao thì thị trường càng biến động mạnh.

---

## 8.4 Rolling correlation

```text
rolling_corr    = corr(stock_close, oil_price)
stock_gold_corr = corr(stock_close, gold_price)
```

Ý nghĩa:

| Correlation | Diễn giải           |
| ----------: | ------------------- |
|       Gần 1 | Đi cùng chiều mạnh  |
|      Gần -1 | Đi ngược chiều mạnh |
|       Gần 0 | Quan hệ yếu         |

---

## 8.5 Alert level

Alert level gồm:

```text
LOW
MEDIUM
HIGH
```

Logic:

```text
MEDIUM nếu:
- abs(oil_return_pct) >= 1.5
hoặc abs(gold_return_pct) >= 1.5
hoặc abs(rolling_corr) >= 0.4
hoặc abs(stock_gold_corr) >= 0.4
```

```text
HIGH nếu:
- oil hoặc gold biến động từ 3% trở lên
và
- correlation giữa stock với oil/gold từ 0.6 trở lên
```

Nếu:

```text
alert_level == HIGH
```

thì:

```text
target_market_risk = 1
```

---

# 9. Thuật toán Machine Learning

Thuật toán được chọn:

```text
RandomForestClassifier trong Spark MLlib
```

Lý do chọn:

* Dễ giải thích.
* Phù hợp dữ liệu dạng bảng.
* Có thể train trực tiếp bằng Spark.
* Có feature importance.
* Không cần scale feature phức tạp.
* Thể hiện được sức mạnh Spark MLlib trong xử lý dữ liệu lớn.

Tham số đang dùng:

```text
RF_NUM_TREES=150
RF_MAX_DEPTH=5
RF_MIN_INSTANCES_PER_NODE=10
USE_CLASS_WEIGHT=false
TRAIN_RATIO=0.8
```

Chạy training:

```powershell
$env:ML_LABEL_COL = 'target_market_risk'
$env:ML_SYMBOL_FILTER = 'AAPL,MSFT,TSLA'
$env:USE_CLASS_WEIGHT = 'false'
$env:RF_NUM_TREES = '150'
$env:RF_MAX_DEPTH = '5'
$env:RF_MIN_INSTANCES_PER_NODE = '10'
python .\src\spark_ml_train.py
```

---

# 10. Kết quả training hiện tại

Model được train trên nhóm:

```text
AAPL, MSFT, TSLA
```

Kết quả mẫu:

```text
baseline_accuracy        = 0.7557
accuracy                 = 0.9481
AUC                      = 0.9836
F1                       = 0.9476
improvement_over_baseline = +0.1923
```

Diễn giải:

```text
Nếu luôn đoán class phổ biến nhất là HIGH RISK, baseline đạt khoảng 75.57%.
RandomForestClassifier đạt khoảng 94.81%.
Model tốt hơn baseline khoảng 19.23%.
```

Các file kết quả:

```text
models/stock_direction_rf
models/stock_direction_rf_meta.json
output/ml_evaluation.csv
output/ml_feature_importance.csv
output/ml_predictions_sample.csv
```

---

# 11. Hướng dẫn chạy offline/batch trên Windows

## Bước 1: Chuẩn bị môi trường

Mở Windows PowerShell tại thư mục dự án rồi chạy:

```powershell
Set-Location 'C:\A\BA_BTL'
Set-ExecutionPolicy -Scope Process Bypass
.\setup_windows.ps1
.\.venv\Scripts\Activate.ps1
```

Script tạo `.venv`, cài `requirements.txt` và kiểm tra Python/Java. Dự án cần JDK 17 hoặc 21 (64-bit). Nếu đã có môi trường và chỉ muốn kiểm tra:

```powershell
.\setup_windows.ps1 -SkipPackageInstall
```

Apache Kafka cần được giải nén riêng. Đặt `KAFKA_HOME` tới thư mục Kafka, không phải thư mục `bin`:

```powershell
[Environment]::SetEnvironmentVariable('KAFKA_HOME', 'C:\tools\kafka', 'User')
```

Sau đó đóng và mở lại PowerShell.

---

## Bước 2: Cấu hình Java và PySpark cho phiên PowerShell hiện tại

```powershell
$JavaExe = (Get-Command java.exe).Source
$env:JAVA_HOME = Split-Path -Parent (Split-Path -Parent $JavaExe)
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
$env:PYSPARK_PYTHON = (Resolve-Path '.\.venv\Scripts\python.exe').Path
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:SPARK_LOCAL_IP = '127.0.0.1'
```

Nếu `java` chưa có trong `PATH`, đặt `JAVA_HOME` thủ công theo đường dẫn JDK thực tế. Nếu `JAVA_HOME` đã đúng thì không cần gán lại ba dòng đầu.

Kiểm tra:

```powershell
java -version
Get-Command python
python --version
$env:PYSPARK_PYTHON
$env:PYSPARK_DRIVER_PYTHON
```

---

## Bước 3: Tải dữ liệu

```powershell
python generate_sample_data.py
```

Nếu thư mục `data/` đã có đủ dữ liệu thì có thể bỏ qua bước này.

---

## Bước 4: Chạy ETL

```powershell
python etl_pipeline.py
```

Output:

```text
output/market_data_final.csv
```

---

## Bước 5: Kiểm tra dữ liệu

```powershell
python preview_data.py
```

---

## Bước 6: Tạo feature MapReduce-style

```powershell
python mapreduce_analysis.py
```

Output:

```text
output/market_analysis_results.csv
output/market_analysis_results_weekly.csv
output/market_analysis_results_monthly_volatility.csv
```

---

## Bước 7: Train Spark ML model

```powershell
$env:ML_LABEL_COL = 'target_market_risk'
$env:ML_SYMBOL_FILTER = 'AAPL,MSFT,TSLA'
$env:USE_CLASS_WEIGHT = 'false'
$env:RF_NUM_TREES = '150'
$env:RF_MAX_DEPTH = '5'
$env:RF_MIN_INSTANCES_PER_NODE = '10'
python .\src\spark_ml_train.py
```

Kiểm tra kết quả:

```powershell
Get-Content .\output\ml_evaluation.csv
Get-Content .\output\ml_feature_importance.csv -TotalCount 10
Get-Content .\output\ml_predictions_sample.csv -TotalCount 10
```

---

# 12. Hướng dẫn chạy streaming demo bằng 4 terminal

Cách nhanh nhất trên Windows là chạy toàn bộ pipeline bằng một lệnh:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\start_clean.ps1
```

Mặc định script chạy pipeline ML. Dùng `.\start_clean.ps1 -Pipeline metrics` cho pipeline metrics cũ, hoặc thêm `-RetrainModel` nếu muốn train lại model. Các bước dưới đây dành cho trường hợp muốn chạy thủ công bằng 4 terminal.

Mục tiêu:

```text
Kafka chạy trước
Spark Streaming Predictor đọc Kafka
Dashboard đọc prediction parquet
Producer gửi dữ liệu demo từng dòng
```

---

## Bước 0: Tạo demo data

```powershell
$env:DEMO_SYMBOLS = 'AAPL,MSFT,TSLA'
$env:DEMO_ROWS_PER_SYMBOL = '120'
$env:DEMO_START_DATE = '2024-01-01'
python .\create_demo_data.py
```

Kiểm tra:

```powershell
$Demo = Import-Csv .\output\market_data_demo.csv
$Demo.Count
$Demo | Group-Object symbol | Select-Object Name, Count
$Demo | Select-Object -First 5
```

---

## Terminal 1: Kafka

```powershell
.\.venv\Scripts\Activate.ps1
$KafkaBin = Join-Path $env:KAFKA_HOME 'bin\windows'
& "$KafkaBin\kafka-server-start.bat" .\kraft.properties
```

Giữ terminal này mở.

---

## Terminal 2: Spark Streaming Predictor

```powershell
.\.venv\Scripts\Activate.ps1
$env:PYSPARK_PYTHON = (Resolve-Path '.\.venv\Scripts\python.exe').Path
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:SPARK_LOCAL_IP = '127.0.0.1'

Remove-Item -LiteralPath 'output\parquet\bronze_market_data' -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath 'output\parquet\market_predictions' -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath 'checkpoints\spark_market_predictions' -Recurse -Force -ErrorAction SilentlyContinue

$env:STREAM_SYMBOL_FILTER = 'AAPL,MSFT,TSLA'
$env:KAFKA_TOPIC = 'market_data'
$env:KAFKA_STARTING_OFFSETS = 'latest'
$env:SPARK_TRIGGER_INTERVAL = '2 seconds'
$env:ROLLING_WINDOW = '10'
python .\src\spark_stream_predictor.py
```

Giữ terminal này mở.

Nếu muốn nhận tất cả symbol thì bỏ `STREAM_SYMBOL_FILTER`.

---

## Terminal 3: Dashboard

```powershell
.\.venv\Scripts\Activate.ps1
$env:STREAMLIT_REFRESH_EVERY = '1s'
$env:LOOKBACK_DAYS = '90'
$env:TAIL_ROWS = '120'
python -m streamlit run .\app_ml.py
```

Mở:

```text
http://localhost:8501
```

Dashboard có dropdown chọn symbol. Nếu dropdown chưa hiện đủ symbol, bấm:

```text
Làm mới danh sách symbol
```

---

## Terminal 4: Kafka Producer

```powershell
.\.venv\Scripts\Activate.ps1
$env:MARKET_CSV_PATH = 'output/market_data_demo.csv'
$env:STREAM_SLEEP_SECONDS = '1'
python .\src\producer_kafka.py
```

Nếu muốn producer gửi nhanh hơn:

```powershell
$env:MARKET_CSV_PATH = 'output/market_data_demo.csv'
$env:STREAM_SLEEP_SECONDS = '0.3'
python .\src\producer_kafka.py
```

---

# 13. Thứ tự chạy bắt buộc

```text
1. Kafka
2. Spark Streaming Predictor
3. Dashboard
4. Producer
```

Lý do:

```text
Spark chạy với KAFKA_STARTING_OFFSETS=latest,
nên Spark chỉ đọc message mới sau khi Spark đã chạy.
```

Nếu producer chạy trước Spark, Spark sẽ bỏ qua message cũ.

---

# 14. Chạy lại demo từ đầu

Dừng Terminal 2, 3, 4 bằng `Ctrl + C`.

Giữ Kafka cũng được.

Xóa output streaming:

```powershell
Remove-Item -LiteralPath 'output\parquet\bronze_market_data' -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath 'output\parquet\market_predictions' -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath 'checkpoints\spark_market_predictions' -Recurse -Force -ErrorAction SilentlyContinue
```

Tạo lại demo data nếu cần:

```powershell
$env:DEMO_SYMBOLS = 'AAPL,MSFT,TSLA'
$env:DEMO_ROWS_PER_SYMBOL = '120'
$env:DEMO_START_DATE = '2024-01-01'
python .\create_demo_data.py
```

Sau đó chạy lại theo thứ tự:

```text
Terminal 2: Spark Streaming Predictor
Terminal 3: Dashboard
Terminal 4: Producer
```

---

# 15. Kiểm tra prediction parquet

Chạy:

```powershell
@'
import pandas as pd
from pathlib import Path

path = Path("output/parquet/market_predictions")

if not path.exists():
    print("No market_predictions yet.")
else:
    df = pd.read_parquet(path)
    print("Rows:", len(df))
    print("Symbols:", sorted(df["symbol"].unique().tolist()))
    print("Date range:", df["market_date"].min(), "->", df["market_date"].max())
    print(df[[
        "market_date",
        "symbol",
        "stock_close",
        "prediction_label",
        "probability_high_risk",
        "alert_level",
    ]].tail(20))
'@ | python -
```

---

# 16. Troubleshooting

## 16.1 Spark báo empty batch liên tục

Nếu Spark log:

```text
Batch N: empty batch, skipped.
```

thì có thể producer chưa gửi dữ liệu hoặc producer gửi symbol không khớp filter.

Ví dụ Spark filter:

```powershell
$env:STREAM_SYMBOL_FILTER = 'TSLA'
```

nhưng producer gửi:

```text
AAPL
```

thì Spark sẽ lọc bỏ hết.

Cách sửa:

```text
Producer gửi AAPL → STREAM_SYMBOL_FILTER="AAPL"
Producer gửi TSLA → STREAM_SYMBOL_FILTER="TSLA"
Producer gửi AAPL,MSFT,TSLA → STREAM_SYMBOL_FILTER="AAPL,MSFT,TSLA"
```

---

## 16.2 Dashboard dropdown chỉ có 1 symbol

Lý do là `output/parquet/market_predictions` hiện chỉ có prediction của 1 symbol.

Cách sửa:

1. Tạo `market_data_demo.csv` nhiều symbol:

```powershell
$env:DEMO_SYMBOLS = 'AAPL,MSFT,TSLA'
python .\create_demo_data.py
```

2. Chạy Spark filter nhiều symbol:

```powershell
$env:STREAM_SYMBOL_FILTER = 'AAPL,MSFT,TSLA'
python .\src\spark_stream_predictor.py
```

3. Chạy producer lại.

4. Bấm **Làm mới danh sách symbol** trên dashboard.

---

## 16.3 Dashboard chưa có dữ liệu

Kiểm tra:

```powershell
Get-ChildItem .\output\parquet\market_predictions
```

Nếu chưa có, nghĩa là Spark chưa ghi prediction.

Kiểm tra producer có đang gửi không:

```text
Đã gửi: YYYY-MM-DD | SYMBOL | Stock: ...
```

---

## 16.4 Lỗi Python/Spark khác môi trường

Nên chạy bằng virtual environment của dự án:

```powershell
.\.venv\Scripts\Activate.ps1
```

Set PySpark Python:

```powershell
$env:PYSPARK_PYTHON = (Resolve-Path '.\.venv\Scripts\python.exe').Path
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:SPARK_LOCAL_IP = '127.0.0.1'
```

Chạy Streamlit bằng:

```powershell
python -m streamlit run app_ml.py
```

Không nên chạy bằng `streamlit run app_ml.py` nếu lệnh `streamlit` trỏ sang Python khác.

---

## 16.5 Kafka port bị chiếm

Kiểm tra:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 9092
```

Dừng Kafka cũ:

```powershell
Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match 'kafka-server-start|kafka\.Kafka' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```

---

# 17. Kịch bản thuyết trình đề xuất

## Bước 1: Giới thiệu bài toán

```text
Nhóm xây dựng hệ thống phân loại trạng thái rủi ro hiện tại của thị trường:
HIGH RISK hoặc NORMAL.
```

## Bước 2: Giới thiệu MapReduce

```text
MAP:
    Tính return và lag feature cho từng dòng dữ liệu.

REDUCE:
    Gom theo symbol/time window để tính moving average, volatility,
    rolling correlation và alert level.
```

## Bước 3: Giới thiệu Spark MLlib

```text
Spark MLlib train RandomForestClassifier trên feature dataset.
Model phân loại current market risk.
```

## Bước 4: Giới thiệu Kafka + Spark Streaming

```text
Kafka producer mô phỏng realtime market data.
Spark Streaming đọc Kafka, tính feature realtime, load model và predict.
```

## Bước 5: Dashboard

```text
Dashboard hiển thị:
- Current Market State: HIGH RISK / NORMAL
- Risk probability
- Stock/Oil/Gold chart
- Correlation chart
- Feature importance
- Model evaluation
```

---

# 18. Cách giải thích kết quả ML

Kết quả hiện tại:

```text
Baseline accuracy: 75.57%
Model accuracy:    94.81%
AUC:               98.36%
F1:                94.76%
Improvement:       +19.23%
```

Có thể nói:

```text
Baseline là cách đoán đơn giản luôn chọn class phổ biến nhất.
Model RandomForestClassifier học từ các feature MapReduce/Spark tạo ra
và đạt kết quả cao hơn baseline khoảng 19.23%.
```

---

# 19. Kết luận

Dự án mô phỏng pipeline Big Data và BI hoàn chỉnh:

```text
Data Collection
    ↓
ETL
    ↓
MapReduce-style Feature Engineering
    ↓
Spark MLlib Training
    ↓
Kafka Streaming
    ↓
Spark Structured Streaming Inference
    ↓
Streamlit BI Dashboard
```

Dự án thể hiện các nội dung chính của môn học:

```text
Big Data
MapReduce
Spark
Spark MLlib
Kafka Streaming
Business Intelligence Dashboard
```

Mục tiêu cuối cùng là hỗ trợ người dùng theo dõi trạng thái rủi ro thị trường realtime và đưa ra cảnh báo HIGH RISK / NORMAL dựa trên dữ liệu stock, oil và gold.

```
```
