package com.rueggerllc.flink.tests;


import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.producers.socket.SocketProducerServer;
import com.rueggerllc.flink.stream.producers.socket.SocketProducerStrategy;


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
	public void testStream() {
		try {
			SocketProducerServer server = new SocketProducerServer(new SocketProducerStrategy("input/sensorstream1.txt",false));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	@Test
	@Ignore
	public void testStream131616() {
		try {
			SocketProducerServer server = new SocketProducerServer(new SocketProducerStrategy("input/sensorstream131316.txt",true));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	@Test
	@Ignore
	public void testStream131619() {
		try {
			SocketProducerServer server = new SocketProducerServer(new SocketProducerStrategy("input/sensorstream131619.txt",true));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	@Test
	@Ignore
	public void testTreadmillCounter() {
		try {
			SocketProducerServer server = new SocketProducerServer(new SocketProducerStrategy("input/treadmill.txt",true));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	@Test
	// @Ignore
	public void testWordCount() {
		try {
			SocketProducerServer server = new SocketProducerServer(new SocketProducerStrategy("input/words.txt",false));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	

}