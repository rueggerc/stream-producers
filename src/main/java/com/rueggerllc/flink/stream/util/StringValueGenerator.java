package com.rueggerllc.flink.stream.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

public class StringValueGenerator implements ValueGenerator {
	
	private static final Logger logger =  Logger.getLogger(StringValueGenerator.class);
	private String key;
	private List<String> possibleValues = new ArrayList<String>();
	

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
	public void setPossibleValues(String[] tokens) {
		for (int i = 2; i < tokens.length; i++) {
			possibleValues.add(tokens[i]);
		}
	}

	public String getValue() {
		Random random = new Random();
		int index = random.nextInt(possibleValues.size());
		return possibleValues.get(index);
	}

}
