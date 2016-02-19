package com.ctrip.kafka;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.yammer.metrics.core.MetricName;

public class JsonUtilsTest {

	@Test
	public void testJson() {
		MetricName metricName = new MetricName("group", "type", "name");
		Map<String, Number> dimensions = new HashMap<>();
		MetricNameParser parser = new MetricNameParser();
		Date time = new Date();
		String metricType = "metricType";
		KafkaMetricItem item = new KafkaMetricItem(metricName, dimensions, parser, time, metricType);
		String output = null;
		try {
			output = JsonUtils.toString(item);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(output);
	}
}
