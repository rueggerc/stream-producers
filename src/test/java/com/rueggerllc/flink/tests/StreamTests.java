package com.rueggerllc.flink.tests;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.producers.socket.ContinuousSocketProducerStrategy;
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
	public void testSendContinuousMessages() {
		try {
			SocketProducerServer server = new SocketProducerServer(new ContinuousSocketProducerStrategy("input/continuous1.txt",true));
			server.execute();
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	
	@Test
	// @Ignore
	public void testReadFile() {
		try {
			String filePath = "input/raspberrypi.txt";
			BufferedReader reader = null;
			InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
			if (is == null) {
				throw new Exception("File Not Found: " + filePath);
			}
			reader = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line=reader.readLine()) != null) {
				logger.info(line);
			}
			close(reader);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	private void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}
	
	
	

}