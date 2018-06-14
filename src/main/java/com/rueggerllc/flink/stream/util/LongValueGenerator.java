package com.rueggerllc.flink.stream.util;

import java.util.Random;

import org.apache.log4j.Logger;

public class LongValueGenerator implements ValueGenerator {
	
	private static final Logger logger =  Logger.getLogger(LongValueGenerator.class);
	private String key;
	private long minValue;
	private long maxValue;
	
	public long getMinValue() {
		return minValue;
	}

	public void setMinValue(long minValue) {
		this.minValue = minValue;
	}

	public long getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(long maxValue) {
		this.maxValue = maxValue;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		Random random = new Random();
		long range = maxValue - minValue;
		long value = minValue + random.nextInt((int)range);
		return String.valueOf(value);
	}

}
