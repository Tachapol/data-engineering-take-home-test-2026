# Streaming Pipeline

## Overview
This project is a real-time streaming pipeline. Order events are produced to Kafka, and Spark Structured Streaming processes these events in real time to calculate order counts per product using 1-minute windows.

## File Structure

```
problem-2/
├── data_simulator.py            # Mock event producer
├── streaming_pipeline.py        # Spark Structured Streaming app
├── doc/                         # Docs and requirements
│   └── requirements.txt
├── results/                     # Output problme 2
└── README.MD                    # This file
```

---

## Project Flow

1. **Order Data Simulator**
   - Generates mock order events in JSON format
   - Sends events to Kafka topic `orders` in real time

2. **Kafka**
   - Acts as a message broker between producer and consumer

3. **Spark Structured Streaming**
   - Reads order events from Kafka
   - Applies event-time windowing (1 minute)
   - Aggregates order count per product
   - Outputs results to the console

---

## **How to Run**

#### Step 1: Start Kafka
Start Kafka and related services using Docker `docker compose up -d`
and make sure the Kafka topic orders is available before running the pipeline.

#### Step 2: Run Order Data Simulator
Open a new terminal and start the data simulator
`python3 data_simulator.py`. This script will continuously generate and send order events to Kafka.

#### Step 3: Run Streaming Pipeline
In another terminal, start the Spark Structured Streaming application
 `python3 streaming_pipeline.py --output console`. The application will consume events from Kafka and print aggregated results to the console in real time.

 