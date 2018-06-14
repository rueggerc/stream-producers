package com.rueggerllc.flink.stream.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

public class ValueFactory {
	
	private static final Logger logger = Logger.getLogger(ValueFactory.class);
	
	private long numberOfElements;
	private long delay;
	private String key;
	private long numberOfKeys;
	private List<ValueGenerator> valueGenerators = new ArrayList<ValueGenerator>();
	private long messageCount = 0;
	
	
	public long getNumberOfElements() {
		return numberOfElements;
	}
	public void setNumberOfElements(long numberOfElements) {
		this.numberOfElements = numberOfElements;
	}
	public long getDelay() {
		return delay;
	}
	public void setDelay(long delay) {
		this.delay = delay;
	}
	public List<ValueGenerator> getValueGenerators() {
		return valueGenerators;
	}
	public void setValueGenerators(List<ValueGenerator> valueGenerators) {
		this.valueGenerators = valueGenerators;
	}
	
	public void addValueGenerator(ValueGenerator valueGenerator) {
		valueGenerators.add(valueGenerator);
	}
	
	public int getDelayValue() {
		int value = (int)delay;
		return value;
	}

	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public long getNumberOfKeys() {
		return numberOfKeys;
	}
	public void setNumberOfKeys(long numberOfKeys) {
		this.numberOfKeys = numberOfKeys;
	}
	public String getNextMessage() throws Exception {
		if (messageCount == numberOfElements) {
			return null;
		}
		StringBuilder buffer = new StringBuilder();
		buffer.append(getKeyValue());
		String sep=",";
		for (ValueGenerator valueGenerator : valueGenerators) {
			buffer.append(sep + valueGenerator.getValue());
		}
		messageCount++;
		return buffer.toString();
	}
	
	private String getKeyValue() {
		Random random = new Random();
		long keyNumber = random.nextInt((int)numberOfKeys);
		return getKey() + keyNumber;
	}
	
	public void createValueGenerator(String[] tokens) {
		String type = tokens[1];
		if (type.equals("double")) {
			DoubleValueGenerator valueGenerator = new DoubleValueGenerator();
			valueGenerator.setKey(tokens[0]);
			valueGenerator.setMinValue(Double.valueOf(tokens[2]));
			valueGenerator.setMaxValue(Double.valueOf(tokens[3]));
			addValueGenerator(valueGenerator);
		} else if (type.equals("long")) {
			LongValueGenerator valueGenerator = new LongValueGenerator();
			valueGenerator.setKey(tokens[0]);
			valueGenerator.setMinValue(Long.valueOf(tokens[2]));
			valueGenerator.setMaxValue(Long.valueOf(tokens[3]));
			addValueGenerator(valueGenerator);
		} else if (type.equals("string")) {
			StringValueGenerator valueGenerator = new StringValueGenerator();
			valueGenerator.setKey(tokens[0]);
			valueGenerator.setPossibleValues(tokens);
			addValueGenerator(valueGenerator);			
		}
	}
	
	
	

}
