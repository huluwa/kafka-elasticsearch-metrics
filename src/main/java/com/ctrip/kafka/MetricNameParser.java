package com.ctrip.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.MetricName;

public class MetricNameParser {

	public static final String SUFFIX_FOR_ALL = "_all";

	public static final Map<String, String> UNKNOWN_TAG = new HashMap<String, String>();
	static {
		UNKNOWN_TAG.put("clientId", "unknown");
	}

	public static final Map<String, String> EMPTY_TAG = new HashMap<String, String>();

	public static final Map<String, Pattern> tagRegexMap = new ConcurrentHashMap<String, Pattern>();

	static {
		tagRegexMap.put("BrokerTopicMetrics", Pattern.compile(".*topic=.*"));
		tagRegexMap.put("DelayedProducerRequestMetrics", Pattern.compile(".*topic=.*"));

		tagRegexMap.put("ProducerTopicMetrics", Pattern.compile(".*topic=.*"));
		tagRegexMap.put("ProducerRequestMetrics", Pattern.compile(".*brokerHost=.*"));

		tagRegexMap.put("ConsumerTopicMetrics", Pattern.compile(".*topic=.*"));
		tagRegexMap.put("FetchRequestAndResponseMetrics", Pattern.compile(".*brokerHost=.*"));
		tagRegexMap.put("ZookeeperConsumerConnector",
		      Pattern.compile(".*name=OwnedPartitionsCount,.*topic=.*|^((?!name=OwnedPartitionsCount).)*$"));
	}

	static final Logger log = LoggerFactory.getLogger(MetricNameParser.class);

	private String name;

	private Map<String, String> tags;

	public String getName() {
		return name;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void parse(MetricName metricName) {
		Pattern p = tagRegexMap.get(metricName.getType());
		if (p != null && !p.matcher(metricName.getMBeanName()).matches()) {
			name = MetricNameFormatter.format(metricName, SUFFIX_FOR_ALL);
		} else {
			name = MetricNameFormatter.format(metricName);
		}
		tags = parseTags(metricName);
	}

	private Map<String, String> parseTags(MetricName metricName) {
		Map<String, String> tags = EMPTY_TAG;
		if (metricName.hasScope()) {
			final String name = metricName.getName();
			final String mBeanName = metricName.getMBeanName();
			final int idx = mBeanName.indexOf(name);
			if (idx < 0) {
				log.error("Cannot find name[{}] in MBeanName[{}]", name, mBeanName);
			} else {
				String tagStr = mBeanName.substring(idx + name.length() + 1);
				if ("kafka.producer".equals(metricName.getGroup()) && !tagStr.contains("clientId")) {
					tagStr = "clientId=unknown,".concat(tagStr);
				}
				if (tagStr.length() > 0) {
					// tags = tagStr.replace('=', ':').split(",");
					String[] tagsArray = tagStr.split(",");
					for (String tagItem : tagsArray) {
						String[] item = tagItem.split("=");
						tags.put(item[0], item[1]);
					}
				}
			}
		} else if ("kafka.producer".equals(metricName.getGroup())) {
			tags = UNKNOWN_TAG;
		}
		return tags;
	}
}
