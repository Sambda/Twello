from kafka import KafkaAdminClient

kafka_admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

print(kafka_admin.describe_cluster())

