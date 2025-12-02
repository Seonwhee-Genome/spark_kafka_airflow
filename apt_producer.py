# file: apt_producer.py
import json
import time
from kafka import KafkaProducer

TOPIC = "apt_topic"
JSON_PATH = "./seoul_apt_info.json"  # JSON 파일 경로

def main():
    # 한글이 포함되어 있으므로 ensure_ascii=False 로 직렬화
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    # JSON 파일 로드
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    # 데이터 배열 꺼내기: {"DESCRIPTION": {...}, "DATA": [ ... ]}
    records = payload.get("DATA", [])
    print(f"[INFO] 총 {len(records)}건 전송 시작")

    # 각 레코드를 그대로 전송
    for i, record in enumerate(records, start=1):
        producer.send(TOPIC, value=record)
        
        time.sleep(1)
        print(f"[SENT] {i}/{len(records)}: {record}")

    producer.flush()
    print("[DONE] 모든 메시지 전송 완료")

if __name__ == "__main__":
    main()
