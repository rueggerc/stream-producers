package com.rueggerllc.flink.tests;


import java.util.Calendar;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rueggerllc.flink.stream.beans.CarSensorReading;


public class CarSensorTests {

	private static Logger logger = Logger.getLogger(CarSensorTests.class);


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
	// @Ignore
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