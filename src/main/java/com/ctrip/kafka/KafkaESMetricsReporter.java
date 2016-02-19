package com.ctrip.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;

public class KafkaESMetricsReporter implements KafkaESMetricsReporterMBean, KafkaMetricsReporter {

	private static final Logger log = LoggerFactory.getLogger(KafkaESMetricsReporter.class);

	public static final int DEFAULT_METRICS_POLLING_INTERVAL_SECS = 60;

	private TransportClient client;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private long pollingPeriodInSeconds;

	private boolean enabled;

	private AbstractPollingReporter underlying = null;

	private String esClusterName;

	private String esTransportAddress;

	private String esIndexPrefix;

	// private TransportClient createESClient2() throws UnknownHostException {
	// Settings settings = Settings.settingsBuilder().put("cluster.name", esClusterName).build();
	// TransportClient client = TransportClient.builder().settings(settings).build();
	// String[] transportAddress = esTransportAddress.split(",");
	// for (int i = 0; i < transportAddress.length; i++) {
	// String[] split = transportAddress[i].split(":");
	// InetAddress address = InetAddress.getByName(split[0]);
	// int port = Integer.parseInt(split[1]);
	// InetSocketTransportAddress socketAddress = new InetSocketTransportAddress(address, port);
	// client.addTransportAddress(socketAddress);
	// }
	// return client;
	// }

	private TransportClient createESClient() throws UnknownHostException {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", esClusterName).build();
		TransportClient client = new TransportClient(settings);
		String[] transportAddress = esTransportAddress.split(",");
		for (int i = 0; i < transportAddress.length; i++) {
			String[] split = transportAddress[i].split(":");
			InetAddress address = InetAddress.getByName(split[0]);
			int port = Integer.parseInt(split[1]);
			InetSocketTransportAddress socketAddress = new InetSocketTransportAddress(address, port);
			client.addTransportAddress(socketAddress);
		}
		return client;
	}

	@Override
	public String getMBeanName() {
		return "kafka:type=" + getClass().getName();
	}

	@Override
	public void init(VerifiableProperties props) {
		esClusterName = props.getString(MetricConfigConst.ES_CLUSTER_NAME);
		esTransportAddress = props.getString(MetricConfigConst.ES_TRANSPORT_ADDRESS);
		pollingPeriodInSeconds = props.getInt(MetricConfigConst.KAFKA_METRICS_POLLING_INTERVAL_SECS,
		      DEFAULT_METRICS_POLLING_INTERVAL_SECS);
		enabled = props.getBoolean(MetricConfigConst.KAFKA_ES_REPORTER_ENABLED, false);
		esIndexPrefix = props.getString(MetricConfigConst.ES_INDEX_PREFIX, "kafka-es");

		if (enabled) {
			log.info("Reporter is enabled and starting...");
			startReporter(pollingPeriodInSeconds);
		} else {
			log.warn("Reporter is disabled");
		}
	}

	@Override
	public void startReporter(long pollingPeriodInSeconds) {
		if (pollingPeriodInSeconds <= 0) {
			throw new IllegalArgumentException("Polling period must be greater than zero");
		}

		synchronized (running) {
			if (running.get()) {
				log.warn("Reporter is already running");
			} else {
				try {
					client = createESClient();
					underlying = new ElasticSearchReporter(Metrics.defaultRegistry(), client, esIndexPrefix);
					underlying.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
					log.info("Started Reporter with es_cluster_name={}, es_transport_address={}, polling_period_secs={}",
					      esClusterName, esTransportAddress, pollingPeriodInSeconds);
					running.set(true);
				} catch (Exception e) {
					log.warn("Reporter start failed", e);
				}
			}
		}
	}

	@Override
	public void stopReporter() {
		if (!enabled) {
			log.warn("Reporter is disabled");
		} else {
			synchronized (running) {
				if (running.get()) {
					underlying.shutdown();
					client.close();
					running.set(false);
					log.info("Stopped Reporter");
				} else {
					log.warn("Reporter is not running");
				}
			}
		}

	}
}