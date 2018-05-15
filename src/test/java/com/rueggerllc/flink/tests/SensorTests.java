package com.rueggerllc.flink.tests;


import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.producers.ProducerStrategy1;
import com.rueggerllc.flink.stream.producers.ProducerStrategy2;
import com.rueggerllc.flink.stream.producers.SensorSocketServer;


public class SensorTests {

	private static Logger logger = Logger.getLogger(SensorTests.class);


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
	public void testSensorTestCase1() {
		try {
			SensorSocketServer server = new SensorSocketServer(new ProducerStrategy1());
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	@Test
	@Ignore
	public void testSensorTestCase2NoDelay() {
		try {
			boolean simulateDelay = false;
			SensorSocketServer server = new SensorSocketServer(new ProducerStrategy2(simulateDelay));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	@Test
	// @Ignore
	public void testSensorTestCase2WithDelay() {
		try {
			boolean simulateDelay = true;
			SensorSocketServer server = new SensorSocketServer(new ProducerStrategy2(simulateDelay));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	

}