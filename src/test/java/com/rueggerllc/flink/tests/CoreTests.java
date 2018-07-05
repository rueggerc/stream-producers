package com.rueggerllc.flink.tests;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rueggerllc.flink.stream.beans.CarSensorReading;
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
	
	
	@Test
	@Ignore
	public void testGetEventTime() {
		long timeMilliseconds = Calendar.getInstance().getTimeInMillis();
		long timeSeconds = java.time.Instant.now().getEpochSecond();
		
		logger.info("timeMillseconds=" + timeMilliseconds);
		logger.info("timeSeconds=" + timeSeconds);
		
	}
	
	
	@Test
	@Ignore
	public void testParseRawData() {
		String record = "id=sensor1 event=car period=0 timestamp=1526166109624 timestamph=05/12/2018 19:01:49";
		String[] tokens = record.toLowerCase().split(" ");
		
		String idToken = tokens[0];
		String eventToken = tokens[1];
		String periodToken = tokens[2];
		String timestampToken = tokens[3];
		String timestamphToken = tokens[4];
		
		// Get ID
		logger.info("idToken=" + idToken);
		String[] tagValue = idToken.split("=");
		String id = tagValue[1];
		logger.info("ID=" + id);
		
		// Get Timestamp
		logger.info("timestampToken=" + timestampToken);
		tagValue = timestampToken.split("=");
		String timestamp = tagValue[1];
		logger.info("timetamp=" + timestamp);
		
	}
	
	@Test
	@Ignore
	public void testWriteSensorDataToJSON() {
		
		try {
			CarSensorReading reading = new CarSensorReading();
			reading.setId("sensor1");
			reading.setEvent("car");
			reading.setTimestamp(1526166109624L);
			
			ObjectMapper mapper = new ObjectMapper();
			String jsonData = mapper.writeValueAsString(reading);
			// logger.info("JSON=" + jsonData);
			logger.info("JSON=\n" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(reading));
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}	
	
	@Test
	@Ignore
	public void testConvertJSONToCarSensorReading() {
		
		try {
			String jsonData = "{\"id\":\"sensor1\",\"event\":\"car\",\"timestamp\":1526166109624}";
			prettyPrint(jsonData);
			
			ObjectMapper mapper = new ObjectMapper();
			CarSensorReading reading = mapper.readValue(jsonData, CarSensorReading.class);
			logger.info("Reading=\n" + reading);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	private void prettyPrint(String jsonData) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Object json = mapper.readValue(jsonData, Object.class);	
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));		
	}
	
	
	
	
	
	
	

}