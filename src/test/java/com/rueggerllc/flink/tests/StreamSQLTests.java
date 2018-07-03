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

import com.rueggerllc.flink.stream.producers.socket.DiscreteSocketProducerStrategy;
import com.rueggerllc.flink.stream.producers.socket.SocketProducerServer;


public class StreamSQLTests {

	private static Logger logger = Logger.getLogger(StreamSQLTests.class);


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
	public void testPageViewsSmall() {
		try {
			String fileName = "input/pageviewsSmall.txt";
			runDiscreteSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testPageViews() {
		try {
			String fileName = "input/pageviews.txt";
			runDiscreteSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	// @Ignore
	public void testPageViewsBig() {
		try {
			String fileName = "input/pageviewsBig.txt";
			runDiscreteSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testPageViewsHuge() {
		try {
			String fileName = "input/pageviewsHuge.txt";
			runDiscreteSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	private void runDiscreteSocketProducer(String fileName) throws Exception {
		String strategyClassName = DiscreteSocketProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		parms.put("timestamped", "true");
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	
	

}