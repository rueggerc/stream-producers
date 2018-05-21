package com.rueggerllc.flink.tests;


import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class CountWindowTests {

	private static Logger logger = Logger.getLogger(CountWindowTests.class);


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
	public void testCountWindow() {
		try {
//			SocketProducerServer server = new SocketProducerServer(new ProducerStrategyCourses());
//			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	

}