import math
import random
import time
import orjson
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "sensors-raw"
BOOTSTRAP_SERVERS = "localhost:9092"
# SINE WAVE FORMULA FOR SIMULATION: value = base + amp * sin(2 * pi * freq * t) + noise
# OSCILLATING DATA WITH SOME RANDOM NOISE TO SIMULATE REAL SENSOR BEHAVIOR
# base = average starting value 
# amp = how far data can go up/down from base
# freq= how fast it will move
# noise = added to make it real jitter like in real sensors data

SENSORS = {
    1: {"name": "Temperature", "base": 300, "amp": 15, "freq": 0.5, "noise": 0.2},
    2: {"name": "Pressure",    "base": 12,  "amp": 3,  "freq": 1.0, "noise": 0.05},
    3: {"name": "Airflow",     "base": 55,  "amp": 10, "freq": 1.5, "noise": 0.3},
}

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": 1,
    "linger.ms": 5,
    "batch.size": 1024 * 1024,
    "compression.type": "lz4",
    "queue.buffering.max.messages": 1000000,
    "queue.buffering.max.kbytes": 2097152,
    "batch.num.messages": 50000,
    "retries": 3
})

print("\ ULTRA FAST SENSOR PRODUCER STARTED\n")

seq = 1
start_time = time.time()

try:
    while True:
        # TO SEE HOW MUCH TIME HAS PASSED SINCE START TO SIMULATE REAL TIME
        t = time.time() - start_time
        #  CURRENT TIMESTAMP TO ADD IN FLINK FOR WINDOWING
        #  isoformat() turns python datetime object into string 
        now_iso = datetime.now(timezone.utc).isoformat()

        for tag_id, cfg in SENSORS.items():
            value = (
                # SINE WAVE FORMULA FOR SIMULATION
                cfg["base"]
                + cfg["amp"] * math.sin(2 * math.pi * cfg["freq"] * t)
                + random.random() * cfg["noise"]
            )

            payload = {
                "seq": seq,
                "tag_id": tag_id,
                "sensor": cfg["name"],
                "msec": now_iso,  # proper ISO timestamp
                "value": value,
                # will make quality more advanceby integrating websockets 
                # for real time alerts
                "quality": 100 if random.random() > 0.05 else 50,  # 5% chance of lower quality
            }
            # key = str(tag_id).encode("utf-8")
            # produce() async calls

            def delivery_report(err, msg):
                if err:
                    print(f"Delivery failed: {err}") 
            producer.produce(
                TOPIC,
                # key=key,
                # makes python dict into json byte array
                # fills RAM buffer quickly
                value=orjson.dumps(payload)
            )


            seq += 1
        # verifies and move immediately sends data in buffer to Kafka,
        # without this, data would sit in RAM buffer until it fills up or linger time passes
        producer.poll(0)

        if seq % 200000 == 0:
            elapsed = time.time() - start_time
            print(f"{seq} events --> {seq/elapsed:.0f} events/sec")

except KeyboardInterrupt:
    pass

finally:
    producer.flush()
    print("Producer shutdown complete")
