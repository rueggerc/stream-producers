package com.rueggerllc.flink.tests;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.beans.EventBean;
import com.rueggerllc.flink.stream.producers.socket.DiscreteSocketProducerStrategy;
import com.rueggerllc.flink.stream.producers.socket.SocketProducerServer;


public class WatermarkTests {

	private static Logger logger = Logger.getLogger(WatermarkTests.class);


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
	public void testStreaming102() {
		try {
			String fileName = "input/streaming102.txt";
			// boolean timestamped = false;
			// runDiscreteSocketProducer(fileName,timestamped);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	// @Ignore
	public void testReadFile() {
		try {
			String filePath = "input/streaming102.txt";
			BufferedReader reader = null;
			InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
			if (is == null) {
				throw new Exception("File Not Found: " + filePath);
			}
			reader = new BufferedReader(new InputStreamReader(is));
			String line = null;
			List<EventBean> eventBeans = new ArrayList<>();
			while ((line=reader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				// System.out.println(line);
				
				String[] tokens = line.split(",");
				String key = tokens[0];
				String label = tokens[1];
				String value = tokens[2];
				int processTimeOrder = Integer.parseInt(tokens[3]);
				int eventTimeDelay = Integer.parseInt(tokens[4]);
				int processTimeDelay = Integer.parseInt(tokens[5]);
				
				EventBean event = new EventBean();
				event.setKey(key);
				event.setLabel(label);
				event.setValue(value);
				event.setProcessTimeOrder(processTimeOrder);
				event.setEventTimeDelay(eventTimeDelay);
				event.setProcessTimeDelay(processTimeDelay);
				
				System.out.println(event + "\n");
				eventBeans.add(event);
				
						
				
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

	private void runDiscreteSocketProducer(String fileName, boolean timestamped) throws Exception {
		String strategyClassName = DiscreteSocketProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		parms.put("timestamped", String.valueOf(timestamped));
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	

}