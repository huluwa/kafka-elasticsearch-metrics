package com.ctrip.kafka;


public enum Dimension {
	count("count"), meanRate("meanRate"), rate1m("1MinuteRate"), rate5m("5MinuteRate"), rate15m("15MinuteRate"), min(
	      "min"), max("max"), mean("mean"), stddev("stddev"), median("median"), p75("p75"), p95("p95"), p98("p98"), p99(
	      "p99"), p999("p999");

	final String displayName;

	public String getDisplayName() {
		return displayName;
	}

	Dimension(String defaultValue) {
		this.displayName = defaultValue;
	}

}