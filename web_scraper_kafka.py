# web_scraper_kafka.py
import time
import json
from kafka import KafkaProducer
from support_functions import transform_one_tin, convert_np_types, scrape_all_mogi

# Cấu hình Kafka
KAFKA_TOPIC = "mogi-stream"
KAFKA_SERVER = "127.0.0.1:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def run_producer():
    while True:
        row_list = scrape_all_mogi()  # Hàm này trả về list
        if row_list:
            for row_new in row_list:
                result_data = transform_one_tin(row_new)
                if result_data:
                    result_data_clean = convert_np_types(result_data)
                    producer.send(KAFKA_TOPIC, value=result_data_clean)
                    print(f"[PRODUCER] Sent data: {result_data_clean}")
                else:
                    print("[PRODUCER] Transform thất bại!")
        else:
            print("[PRODUCER] Không cào được tin mới!")

        # Nghỉ 60 giây trước khi lặp
        time.sleep(60)


if __name__ == "__main__":
    run_producer()