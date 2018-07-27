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

import com.rueggerllc.flink.stream.producers.socket.EventProducerStrategy;
import com.rueggerllc.flink.stream.producers.socket.SocketProducerServer;


public class SessionWindowTests {

	private static Logger logger = Logger.getLogger(SessionWindowTests.class);


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
	public void testSessionWindow1() {
		try {
			String fileName = "input/sessionwindow1.txt";
			runEventSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	

	private void runEventSocketProducer(String fileName) throws Exception {
		String strategyClassName = EventProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	

}