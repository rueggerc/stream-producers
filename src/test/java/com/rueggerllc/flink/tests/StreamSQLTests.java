package com.rueggerllc.flink.tests;


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
	// @Ignore
	public void testPageViews() {
		try {
			SocketProducerServer server = new SocketProducerServer(new DiscreteSocketProducerStrategy("input/pageviews.txt",true));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	
	

}