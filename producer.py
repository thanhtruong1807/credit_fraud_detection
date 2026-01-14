from kafka import KafkaProducer
import json
import random
import pandas as pd
import time
import uuid
import numpy as np

# ===============================
# CONFIG
# ===============================
CSV_PATH = "./data/clean_test.csv"
TOPIC_NAME = "transaction_data"
FRAUD_RATIO = 0.3     # 30% message là fraud (0.3–0.5 để demo rõ)
SLEEP_SEC = 2         # thời gian giữa các batch
MAX_BATCH = 10        # số message / batch

# ===============================
# DATA GENERATOR
# ===============================
class DataGenerator:
    def __init__(self):
        self.df = pd.read_csv(CSV_PATH, index_col=0)
        print("Loaded columns:", self.df.columns.tolist())

        # Tách fraud / normal
        self.df_fraud = self.df[self.df["is_fraud"] == 1]
        self.df_normal = self.df[self.df["is_fraud"] == 0]

        print(f"Fraud rows: {len(self.df_fraud)}")
        print(f"Normal rows: {len(self.df_normal)}")

    def generate_batch(self):
        batch_size = random.randint(1, MAX_BATCH)
        messages = []

        for _ in range(batch_size):
            # oversample fraud
            if np.random.rand() < FRAUD_RATIO and len(self.df_fraud) > 0:
                row = self.df_fraud.sample(1).iloc[0]
            else:
                row = self.df_normal.sample(1).iloc[0]

            msg = row.to_dict()

            # đảm bảo có txn_id / trans_num
            if "txn_id" not in msg or pd.isna(msg.get("txn_id")):
                msg["txn_id"] = str(uuid.uuid4())

            messages.append(msg)

        return messages


# ===============================
# KAFKA PRODUCER
# ===============================
class MyProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            client_id="fraud-producer",
            acks=1,
            retries=5,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send_messages(self, messages):
        for msg in messages:
            self.producer.send(TOPIC_NAME, value=msg)
        self.producer.flush()

    def close(self):
        self.producer.close()


# ===============================
# MAIN LOOP
# ===============================
if __name__ == "__main__":
    print("Starting Kafka fraud producer...")

    generator = DataGenerator()
    producer = MyProducer()

    try:
        while True:
            batch = generator.generate_batch()
            producer.send_messages(batch)

            fraud_cnt = sum(1 for x in batch if x.get("is_fraud", 0) == 1)
            print(
                f"Sent {len(batch)} messages "
                f"(fraud={fraud_cnt}, normal={len(batch)-fraud_cnt})"
            )

            time.sleep(SLEEP_SEC)

    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.close()
