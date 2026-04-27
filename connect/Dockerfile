# Extends the Confluent Kafka Connect image with the Elasticsearch Sink Connector plugin.
# The connector is installed via confluent-hub so no manual JAR management is needed.
FROM confluentinc/cp-kafka-connect:7.6.1

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:14.0.12
