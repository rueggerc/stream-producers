package com.rueggerllc.flink.tests;


import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.producers.socket.ContinuousSocketProducerStrategy;
import com.rueggerllc.flink.stream.producers.socket.DiscreteSocketProducerStrategy;
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
	public void testSensor131316() {
		try {
			String fileName = "input/sensorstream131316.txt";
			boolean timestamped = true;
			runDiscreteSocketProducer(fileName,timestamped);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	// @Ignore
	public void testSensor131619() {
		try {
			String fileName = "input/sensorstream131619.txt";
			boolean timestamped = true;
			runDiscreteSocketProducer(fileName,timestamped);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	
//	@Test
//	@Ignore
//	public void testStream() {
//		try {
//			SocketProducerServer server = new SocketProducerServer(new DiscreteSocketProducerStrategy("input/sensorstream1.txt",false));
//			server.execute();
//		} catch (Exception e) {
//			logger.error("ERROR", e);
//		}
//	}
//	
//	@Test
//	// @Ignore
//	public void testStream131316() {
//		try {
//			SocketProducerServer server = new SocketProducerServer(new DiscreteSocketProducerStrategy("input/sensorstream131316.txt",true));
//			server.execute();
//		} catch (Exception e) {
//			logger.error("ERROR", e);
//		}
//	}
//	
//	@Test
//	@Ignore
//	public void testStream131619() {
//		try {
//			SocketProducerServer server = new SocketProducerServer(new DiscreteSocketProducerStrategy("input/sensorstream131619.txt",true));
//			server.execute();
//		} catch (Exception e) {
//			logger.error("ERROR", e);
//		}
//	}
//	
//	@Test
//	@Ignore
//	public void testSendContinuousMessages() {
//		try {
//			SocketProducerServer server = new SocketProducerServer(new DiscreteSocketProducerStrategy("input/sensorstream131619.txt",true));
//			server.execute();
//		} catch (Exception e) {
//			logger.error("ERROR", e);
//		}		
//	}
//	
//	

	
	
	private void runDiscreteSocketProducer(String fileName, boolean timestamped) throws Exception {
		String strategyClassName = DiscreteSocketProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		parms.put("timestamped", String.valueOf(timestamped));
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	
	

}