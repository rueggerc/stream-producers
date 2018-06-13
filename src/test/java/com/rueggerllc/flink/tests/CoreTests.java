package com.rueggerllc.flink.tests;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DecimalFormat;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.util.DoubleValueGenerator;
import com.rueggerllc.flink.stream.util.ValueFactory;
import com.rueggerllc.flink.stream.util.ValueGenerator;


public class CoreTests {

	private static Logger logger = Logger.getLogger(CoreTests.class);
	private static DecimalFormat decimalFormatter = new DecimalFormat(".##");


	@BeforeClass
	public static void setupClass() throws Exception {
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}

	@Test
	@Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	@Test
	@Ignore
	public void testGenerateValues() {
		try {
			logger.info("Generate Values");
			logger.info("temp=" + getValue(44,100));
			logger.info("temp=" + getValue(44,100));
			logger.info("temp=" + getValue(44,100));
			logger.info("temp=" + getValue(44,100));
			
			logger.info("humidity=" + getValue(70,83));
			logger.info("humidity=" + getValue(70,83));
			logger.info("humidity=" + getValue(70,83));
			logger.info("humidity=" + getValue(70,83));
			
			logger.info("pressure=" + getValue(1000,1102));
			logger.info("pressure=" + getValue(1000,1102));
			logger.info("pressure=" + getValue(1000,1102));
			logger.info("pressure=" + getValue(1000,1102));
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	// @Ignore
	public void testReadFile() {
		try {
			logger.info("MAX INT=" + Integer.MAX_VALUE);
			logger.info("MAX LONG=" + Long.MAX_VALUE);
			
			// Data
			ValueFactory valueFactory = new ValueFactory();
			
			long numberOfElements = 0;
			long delay = 0;
			
			
			String filePath = "input/raspberrypi.txt";
			BufferedReader reader = null;
			InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
			if (is == null) {
				throw new Exception("File Not Found: " + filePath);
			}
			reader = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line=reader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				String[] tokens = line.split(",");
				if (tokens[0].equals("numberOfElements")) {
					numberOfElements = getValue(tokens);
					valueFactory.setNumberOfElements(numberOfElements);
				} else if (tokens[0].equals("delay")) {
					delay = getValue(tokens);
					valueFactory.setDelay(delay);
					
				} else if (tokens[0].equals("key")) {
					valueFactory.setKey(tokens[1]);
				} else if (tokens[0].equals("numberOfKeys")) {
					valueFactory.setNumberOfKeys(getValue(tokens));
				} else {
					getRangedValue(tokens);
					ValueGenerator valueGenerator = getValueGenerator(tokens);
					valueFactory.addValueGenerator(valueGenerator);
				}
			}
			close(reader);
			
			// Generate Values
			logger.info("====== GENERATE MESSAGES BEGIN");
			String msg = null;
			while ((msg = valueFactory.getNextMessage()) != null) {
				logger.info("MSG=" + msg);
			}
			logger.info("====== GENERATE MESSAGES END");
			
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	private long getValue(String line) {
		String[] tokens = line.split(",");
		long value = Long.valueOf(tokens[1]);
		return value;
	}
	
	private long getValue(String[] tokens) {
		long value = Long.valueOf(tokens[1]);
		return value;
	}
	
	public void getRangedValue(String[] tokens) {
		logger.info("Ranged Value");
		logger.info("key=" + tokens[0] + " min=" + tokens[1] + " max=" + tokens[2]);
	}
	
	public ValueGenerator getValueGenerator(String[] tokens) {
		DoubleValueGenerator valueGenerator = new DoubleValueGenerator();
		valueGenerator.setKey(tokens[0]);
		valueGenerator.setMinValue(Double.valueOf(tokens[1]));
		valueGenerator.setMaxValue(Double.valueOf(tokens[2]));
		return valueGenerator;
	}
	
	
	
	private void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}
	
	
	private String getValue(double min, double max) {
		Random random = new Random();
		double value = min + (random.nextDouble()*max);
		return decimalFormatter.format(value);
	}
	
	
	
	
	
	
	
	
	
	

}