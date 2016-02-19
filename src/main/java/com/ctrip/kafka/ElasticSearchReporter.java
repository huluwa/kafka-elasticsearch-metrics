package com.ctrip.kafka;

import static com.ctrip.kafka.Dimension.count;
import static com.ctrip.kafka.Dimension.max;
import static com.ctrip.kafka.Dimension.mean;
import static com.ctrip.kafka.Dimension.meanRate;
import static com.ctrip.kafka.Dimension.median;
import static com.ctrip.kafka.Dimension.min;
import static com.ctrip.kafka.Dimension.p75;
import static com.ctrip.kafka.Dimension.p95;
import static com.ctrip.kafka.Dimension.p98;
import static com.ctrip.kafka.Dimension.p99;
import static com.ctrip.kafka.Dimension.p999;
import static com.ctrip.kafka.Dimension.rate15m;
import static com.ctrip.kafka.Dimension.rate1m;
import static com.ctrip.kafka.Dimension.rate5m;
import static com.ctrip.kafka.Dimension.stddev;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

public class ElasticSearchReporter extends AbstractPollingReporter implements MetricProcessor<Long> {

	private static final Logger log = LoggerFactory.getLogger(ElasticSearchReporter.class);

	private static final SimpleDateFormat INDEX_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");

	public static final String REPORTER_NAME = "kafka-es-metrics";

	protected static final Dimension[] meterDims = { count, meanRate, rate1m, rate5m, rate15m };

	protected static final Dimension[] summarizableDims = { min, max, mean, stddev };

	protected static final Dimension[] SamplingDims = { median, p75, p95, p98, p99, p999 };

	private TransportClient client;

	private String esIndexPrefix;

	private final Clock clock;

	private MetricNameParser parser;

	public ElasticSearchReporter(MetricsRegistry registry, TransportClient client, String esIndexPrefix) {
		super(registry, REPORTER_NAME);
		this.client = client;
		this.esIndexPrefix = esIndexPrefix + "-";
		this.clock = Clock.defaultClock();
	}

	private Boolean isDoubleParsable(final Object o) {
		if (o instanceof Float) {
			return true;
		} else if (o instanceof Double) {
			return true;
		} else if (o instanceof Byte) {
			return false;
		} else if (o instanceof Short) {
			return false;
		} else if (o instanceof Integer) {
			return false;
		} else if (o instanceof Long) {
			return false;
		} else if (o instanceof BigInteger) {
			return false;
		} else if (o instanceof BigDecimal) {
			return true;
		}
		return null;
	}

	@Override
	public void processCounter(MetricName metricName, Counter counter, Long context) {
		Map<String, Number> dimensions = new HashMap<String, Number>();
		dimensions.put("count", counter.count());
		sendToES(metricName, dimensions, parser, "counter");
	}

	@Override
	public void processGauge(MetricName metricName, Gauge<?> gauge, Long context) {
		final Object value = gauge.value();
		final Boolean flag = isDoubleParsable(value);
		if (flag == null) {
			log.debug("Gauge can only record long or double metric, it is " + value.getClass());
		} else {
			Map<String, Number> dimensions = new HashMap<String, Number>();
			dimensions.put("gauge", flag ? new Double(value.toString()) : new Long(value.toString()));
			sendToES(metricName, dimensions, parser, "gauge");
		}
	}

	@Override
	public void processHistogram(MetricName metricName, Histogram histogram, Long context) {
		Map<String, Number> dimensions = new HashMap<String, Number>();
		long count = histogram.count();
		dimensions.put("count", count);

		final Snapshot snapshot = histogram.getSnapshot();
		double[] samplingValues = { snapshot.getMedian(), snapshot.get75thPercentile(), snapshot.get95thPercentile(),
		      snapshot.get98thPercentile(), snapshot.get99thPercentile(), snapshot.get999thPercentile() };
		for (int i = 0; i < samplingValues.length; ++i) {
			dimensions.put(SamplingDims[i].getDisplayName(), samplingValues[i]);
		}

		double[] summarizableValues = { histogram.min(), histogram.max(), histogram.mean(), histogram.stdDev() };
		for (int i = 0; i < summarizableValues.length; ++i) {
			dimensions.put(summarizableDims[i].getDisplayName(), summarizableValues[i]);
		}
		sendToES(metricName, dimensions, parser, "histogram");
	}

	@Override
	public void processMeter(MetricName metricName, Metered meter, Long epoch) {
		Map<String, Number> dimensions = new HashMap<String, Number>();
		double[] meterValues = { meter.count(), meter.meanRate(), meter.oneMinuteRate(), meter.fiveMinuteRate(),
		      meter.fifteenMinuteRate() };
		for (int i = 0; i < meterValues.length; ++i) {
			dimensions.put(meterDims[i].getDisplayName(), meterValues[i]);
		}
		sendToES(metricName, dimensions, parser, "meter");
	}

	@Override
	public void processTimer(MetricName metricName, Timer timer, Long context) throws Exception {
		Map<String, Number> dimensions = new HashMap<String, Number>();
		double[] meterValues = { timer.count(), timer.meanRate(), timer.oneMinuteRate(), timer.fiveMinuteRate(),
		      timer.fifteenMinuteRate() };
		for (int i = 0; i < meterValues.length; ++i) {
			dimensions.put(meterDims[i].getDisplayName(), meterValues[i]);
		}

		double[] summarizableValues = { timer.min(), timer.max(), timer.mean(), timer.stdDev() };
		for (int i = 0; i < summarizableValues.length; ++i) {
			dimensions.put(summarizableDims[i].getDisplayName(), summarizableValues[i]);
		}

		Snapshot snapshot = timer.getSnapshot();
		double[] samplingValues = { snapshot.getMedian(), snapshot.get75thPercentile(), snapshot.get95thPercentile(),
		      snapshot.get98thPercentile(), snapshot.get99thPercentile(), snapshot.get999thPercentile() };
		for (int i = 0; i < samplingValues.length; ++i) {
			dimensions.put(SamplingDims[i].getDisplayName(), samplingValues[i]);
		}
		sendToES(metricName, dimensions, parser, "timer");
	}

	@Override
	public void run() {
		try {
			final long epoch = clock.time() / 1000;
			if (parser == null) {
				parser = new MetricNameParser();
			}
			sendAllKafkaMetrics(epoch);
		} catch (RuntimeException ex) {
			log.error("Failed to send metrics to es", ex);
		}
	}

	private void sendAllKafkaMetrics(long epoch) {
		final Map<MetricName, Metric> allMetrics = new TreeMap<MetricName, Metric>(getMetricsRegistry().allMetrics());
		for (Map.Entry<MetricName, Metric> entry : allMetrics.entrySet()) {
			sendMetricImpl(entry.getKey(), entry.getValue(), epoch);
		}
	}

	private void sendMetricImpl(MetricName metricName, Metric metric, long epoch) {
		log.debug("MBeanName[{}], Group[{}], Name[{}], Scope[{}], Type[{}]", metricName.getMBeanName(),
		      metricName.getGroup(), metricName.getName(), metricName.getScope(), metricName.getType());

		try {
			parser.parse(metricName);
			metric.processWith(this, metricName, epoch);
		} catch (Exception ignored) {
			log.error("Error printing regular metrics:", ignored);
		}
	}

	private void sendToES(MetricName metricName, Map<String, Number> dimensionValue, MetricNameParser parser,
	      String metricType) {
		Date time = new Date(clock.time());
		StringBuilder indexId = new StringBuilder(parser.getName()).append("_").append(clock.time());
		IndexRequestBuilder builder = client.prepareIndex(esIndexPrefix + INDEX_DATE_FORMAT.format(time),
		      metricName.getGroup(), indexId.toString());
		KafkaMetricItem item = new KafkaMetricItem(metricName, dimensionValue, parser, time, metricType);
		try {
			String source = JsonUtils.toString(item);
			IndexResponse response = builder.setSource(source).execute().actionGet();
			if (!response.isCreated()) {
				log.warn("Create index failed, {}", response.getId());
			}
		} catch (Exception e) {
			log.warn("Send to ElasticSearch failed", e);
		}
	}

}
