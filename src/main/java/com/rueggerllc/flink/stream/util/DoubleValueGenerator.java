package com.rueggerllc.flink.stream.util;

import java.text.DecimalFormat;
import java.util.Random;

import org.apache.log4j.Logger;

public class DoubleValueGenerator implements ValueGenerator {
	
	private static final Logger logger =  Logger.getLogger(DoubleValueGenerator.class);
	private static DecimalFormat decimalFormatter = new DecimalFormat(".##");
	private String key;
	private double minValue;
	private double maxValue;
	
	public double getMinValue() {
		return minValue;
	}

	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}

	public double getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(double maxValue) {
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
		double range = maxValue - minValue;
		double value = minValue + (random.nextDouble()*range);
		return decimalFormatter.format(value);
	}

}
