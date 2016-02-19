# kafka-elasticsearch-metrics

Kafka ElasticSearch Metrics Reporter
==============================

This is a simple reporter for kafka using the ElasticSearch.
It works with Kafka 0.9.* and ElasticSearch 1.* version.

Install On Broker
------------

1. Build the `kafka-elasticsearch-metrics-0.1.0-jar-with-dependencies` jar using `mvn package`.
2. Add `kafka-elasticsearch-metrics-0.1.0-jar-with-dependencies` to the `libs/` directory of your kafka broker installation
3. Configure the broker (see the configuration section below)
4. Restart the broker

Configuration
------------

Edit the `server.properties` file of your installation, activate the reporter by setting:

    kafka.metrics.reporters=com.ctrip.kafka.KafkaESMetricsReporter[,kafka.metrics.KafkaCSVMetricsReporter[,....]]
    kafka.es.reporter.enabled=true
    es.cluster.name="es-cluster"
    es.transport.address=server1:port,server2:port
    es.index.prefix="kafka-es"
