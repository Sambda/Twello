from kafka import KafkaAdminClient, KafkaConsumer

kafka_admin = KafkaAdminClient(bootstrap_servers=['localhost:9093'])
kafka_consumer = KafkaConsumer("topic_all_11", auto_offset_reset='earliest', group_id="group_all_11",
                         bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)



print(kafka_admin.describe_cluster())

