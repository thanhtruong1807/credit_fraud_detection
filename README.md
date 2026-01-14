# Real-Time Credit Card Fraud Demo (Kafka → Spark ML → Cassandra → Streamlit)

Mô tả ngắn:
Dự án demo pipeline phát sinh giao dịch qua Kafka, xử lý với Spark ML (model đã train), ghi kết quả vào Cassandra và hiển thị dashboard realtime bằng Streamlit.

🔧 Nội dung repository:
- `producer.py` - generator gửi messages vào Kafka (dùng `data/clean_test.csv`).
- `fraud_detection.ipynb` - consumer + Spark predict → insert vào Cassandra (notebook tương tác).
- `demo.py` / `demo_v1.py` - Streamlit dashboard để giám sát dữ liệu realtime.
- `data/` - chứa các CSV (`clean_train.csv`, `clean_test.csv`, ...).
- `model/` - model Spark (được lưu bởi notebook `model_training.ipynb`).
- `encoders/` - encoder pickle từ bước tiền xử lý.
- `docker-compose.yml` - khởi Cassandra, Zookeeper, Kafka cho môi trường local.
- `requirements.txt` - thư viện Python cần cài (tôi đã thêm file này).

---

## Yêu cầu (Prerequisites) ✅
- Linux / macOS / Windows (WSL)
- Docker & Docker Compose
- Python 3.8+ (khuyến nghị 3.10-3.12)
- Java JDK 8+ (bắt buộc để chạy PySpark)
- (Tùy chọn) Jupyter / VS Code để chạy notebook

---

## Cài đặt nhanh (Quick start) 🚀
1) Khởi services (Cassandra + Zookeeper + Kafka):

```bash
# từ thư mục project
docker-compose up -d
docker ps
```

2) Tạo Keyspace và table Cassandra (mở cqlsh):

```bash
docker exec -it cassandra cqlsh
```
Trong cqlsh chạy:

```sql
CREATE KEYSPACE IF NOT EXISTS bigdata
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
USE bigdata;

CREATE TABLE IF NOT EXISTS transaction_data (
  trans_date_trans_time text,
  cc_num bigint,
  merchant text,
  category text,
  amt decimal,
  first text,
  last text,
  gender text,
  street text,
  city text,
  state text,
  zip int,
  lat decimal,
  long decimal,
  city_pop int,
  job text,
  dob date,
  trans_num text,
  unix_time bigint,
  merch_lat decimal,
  merch_long decimal,
  merchant_label int,
  category_label int,
  gender_label int,
  job_label int,
  is_fraud int,
  is_fraud_prediction int,
  inserted_at timestamp,
  PRIMARY KEY (trans_num)
);
```

3) (Nếu cần) tạo topic Kafka:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic transaction_data --partitions 1 --replication-factor 1
```

4) Tạo virtualenv và cài dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> Lưu ý: nếu gặp lỗi về Java khi cài pyspark, cài OpenJDK (`sudo apt install openjdk-11-jdk`).

5) Chạy Producer (gửi dữ liệu mẫu vào Kafka):

```bash
python3 producer.py
```
Producer đọc `data/clean_test.csv` và gửi messages tới topic `transaction_data`.

6) Chạy consumer + Spark predict -> insert vào Cassandra
- Mở `fraud_detection.ipynb` trong Jupyter hoặc VS Code Notebook và chạy các cells theo thứ tự (đảm bảo Spark load được `./model`).
- Notebook sẽ đọc từ Kafka, dự đoán (`p_fraud`, `is_fraud_prediction`) và chèn JSON vào `transaction_data`.

7) Chạy dashboard Streamlit:

```bash
streamlit run demo.py
# hoặc để mở cổng ngoài
streamlit run demo.py --server.port 8501 --server.address 0.0.0.0
```
Truy cập: http://localhost:8501

Đăng nhập admin (mẫu):
- Tạo file `.streamlit/secrets.toml` với nội dung:

```toml
ADMIN_PASSWORD = "admin"
```


## Kiểm tra & Troubleshooting ⚠️
- Kiểm tra container:
```bash
docker ps
```
- Kiểm tra Kafka logs:
```bash
docker logs kafka
```
- Kiểm tra số row trong Cassandra:
```bash
docker exec -it cassandra cqlsh -e "USE bigdata; SELECT count(*) FROM transaction_data;"
```
- Nếu Streamlit không nhận data: đảm bảo `fraud_detection` notebook/script đang chạy và đã insert dữ liệu vào Cassandra.
- Nếu Spark không load model: kiểm tra `model/` có tồn tại và tương thích với phiên bản PySpark.



# credit_fraud_detection
