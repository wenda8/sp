from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
import time

consumer = Consumer({
    'bootstrap.servers': "localhost:9092",
        'group.id': "iot-sensor-consumer-group",
        'auto.offset.reset': 'earliest'
})

def process_message(msg):
    try:
        return json.loads(msg.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Error decoding message: {e}")
        return None

def main():
    consumer.subscribe(['iot-sensor-topic'])

    type_df = pd.DataFrame(columns=['timestamp', 'type', 'name', 'value'])
    name_df = pd.DataFrame(columns=['timestamp', 'type', 'name', 'value'])

    interval_time = 20
    last_report_time = time.time()

  
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        sensor_data = process_message(msg.value())
        if sensor_data:
            type_df = pd.concat([type_df, pd.DataFrame([sensor_data])], ignore_index=True)
            name_df = pd.concat([name_df, pd.DataFrame([sensor_data])], ignore_index=True)

        current_time = time.time()
        if current_time - last_report_time >= interval_time:
            print("\nAverage Values by Type:")
            print(type_df.groupby('type')['value'].mean())

            print("\nAverage Values by Name:")
            print(name_df.groupby('name')['value'].mean())

            type_df = type_df.iloc[0:0]
            name_df = name_df.iloc[0:0]
            last_report_time = current_time  

    consumer.close()

if __name__ == "__main__":
    main()
