import json
import time
import random
import threading
from confluent_kafka import Producer

TOPIC_NAME = "iot-sensor-topic"

producer = Producer({
    'bootstrap.servers': 'localhost:9092', 
})

class Sensor:
    def __init__(self, sensor_type, sensor_name, send_interval, data_generator):
        self.sensor_type = sensor_type
        self.sensor_name = sensor_name
        self.send_interval = send_interval
        self.data_generator = data_generator

    def generate_data(self):
        value = self.data_generator()
        return {
            'timestamp': time.time(),
            'type': self.sensor_type,
            'name': self.sensor_name,
            'value': round(value, 2)
        }
    
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data(sensor, producer, topic):
    while True:
        data = sensor.generate_data()
        producer.produce(topic, key=sensor.sensor_name, value=json.dumps(data), callback=delivery_report)
        time.sleep(sensor.send_interval)


def uniform_data_generator(min_val, max_val):
    return lambda: random.uniform(min_val, max_val)

def normal_data_generator(mean, std_dev):
    return lambda: random.gauss(mean, std_dev)

sensors = [
    Sensor("Temperature", "TempSensor1", 5, uniform_data_generator(20, 25)),
    Sensor("Temperature", "TempSensor2", 2, normal_data_generator(22, 1)),
    Sensor("Temperature", "TempSensor3", 4, uniform_data_generator(21, 24)),
    Sensor("Pressure", "PresSensor1", 10, uniform_data_generator(1, 3)),
    Sensor("Pressure", "PresSensor2", 5, normal_data_generator(2, 0.5)),
    Sensor("Pressure", "PresSensor3", 1, uniform_data_generator(1.5, 2.5))
]

for sensor in sensors:
    threading.Thread(target=produce_data, args=(sensor, producer, 'iot-sensor-topic')).start()
