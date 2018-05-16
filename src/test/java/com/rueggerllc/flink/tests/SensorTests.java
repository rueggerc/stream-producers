package com.rueggerllc.flink.tests;


import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.producers.socket.ProducerStrategy131316;
import com.rueggerllc.flink.stream.producers.socket.ProducerStrategy131619;
import com.rueggerllc.flink.stream.producers.socket.SensorSocketServer;


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
	// @Ignore
	public void testSensorProducer131616() {
		try {
			SensorSocketServer server = new SensorSocketServer(new ProducerStrategy131316());
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	@Test
	@Ignore
	public void testSensorProducer131619() {
		try {
			SensorSocketServer server = new SensorSocketServer(new ProducerStrategy131619());
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	

}