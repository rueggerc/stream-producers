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
		// Thread.sleep(delay*1000);
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
	
	
	

}
