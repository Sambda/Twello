from kafka import cluster

if __name__ == '__main__':
    metadata = cluster.ClusterMetadata(retry_backoff_ms=100,
                                   metadata_max_age_ms=10000000,
                                   bootstrap_servers='localhost:9092')

    brokerdata = metadata.brokers()
    print(brokerdata)

    print(metadata)

    topics = metadata.topics()
    topics