# Real-Time Data Pipeline Lab

## Overview

This project implements a high-throughput real-time data pipeline simulating industrial sensor data.

## Architecture (Work in Progress)

Producer → Kafka → Flink → Consumer → Database

## Current Progress

* [x] Kafka Producer (high-throughput, batched, compressed)
* [ ] Flink Stream Processing
* [ ] Database Consumer
* [ ] Monitoring & Optimization

## Tech Stack

* Python
* Apache Kafka
* Flink (planned)
* SQL (planned)

## How to Run (Producer Only)

1. Start Kafka on localhost:9092
2. Install dependencies:

   ```
   pip install -r requirements.txt
   ```
3. Run:

   ```
   python producer/producer.py
   ```

## Notes

This repo is being built incrementally to simulate a production-grade streaming pipeline handling high event throughput.
