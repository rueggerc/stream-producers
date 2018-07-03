package com.rueggerllc.flink.tests;


import java.io.Reader;
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


public class StreamTests {

	private static Logger logger = Logger.getLogger(StreamTests.class);


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
	public void tesSensor() {
		try {
			String fileName = "input/raspberrypi.txt";
			boolean timestamped = false;
			runContinuouSocketProducer(fileName, timestamped);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	
	@Test
	// @Ignore
	public void testSessionWindow() {
		try {
			String fileName = "input/sessionstream.txt";
			boolean timestamped = false;
			runDiscreteSocketProducer(fileName,timestamped);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	
	
	
	private void runContinuouSocketProducer(String fileName, boolean timestamped) throws Exception {
		String strategyClassName = ContinuousSocketProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		parms.put("timestamped", String.valueOf(timestamped));
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	private void runDiscreteSocketProducer(String fileName, boolean timestamped) throws Exception {
		String strategyClassName = DiscreteSocketProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		parms.put("timestamped", String.valueOf(timestamped));
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	
	

}