# Twello

Twello is a Kafka supported Twitter Streaming App implemented in Python, that makes it possible to efficiently consume tweets from the Twitter API. It also includes a complete preconfigured monitoring solution using Prometheus and Grafana. Everything is dockerized and can be easily started.

## Getting Started

### Kafka
First start Kafka, including Zookeeper and Jmx-Prometheus-Exporter by running the following Docker Compose File:

`./Kafka/docker-compose.yml`

Kafka can now be advertised by producers and consumers via ***localhost:9092***.

### Monitoring
After that we recommend (it's not mandatory) to start up the monitoring tools Prometheus and Grafana:

`./Metrics/docker-compose.yml`

You can check if everything is running properly by starting up Prometheus running on <localhost:9090> and Grafana running on <localhost:3000>. If you see kafka related metrics in the metrics explorer, everything is working fine.

Thats it, you are rdy to go!

## Twitter Streaming

### Install required Packages

Setup a python environment of your choice, activate it and install the required packages with 

`pip install -r ./requirements.txt`

### Start Producer

python <path_to_file>/Kafka-Stream-Producer.py

### Start Example Consumer

python <path_to_file>/Kafka-Stream-Consumer.py







