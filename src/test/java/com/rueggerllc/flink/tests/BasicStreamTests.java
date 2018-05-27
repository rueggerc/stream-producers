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


public class BasicStreamTests {

	private static Logger logger = Logger.getLogger(BasicStreamTests.class);


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
	public void testWordCount() {
		try {
			SocketProducerServer server = new SocketProducerServer(new SocketProducerStrategy("input/words.txt",false));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	

}