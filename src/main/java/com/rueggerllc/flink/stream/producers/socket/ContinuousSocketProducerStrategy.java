package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.util.DoubleValueGenerator;
import com.rueggerllc.flink.stream.util.Utils;
import com.rueggerllc.flink.stream.util.ValueFactory;
import com.rueggerllc.flink.stream.util.ValueGenerator;

public class ContinuousSocketProducerStrategy extends SocketProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ContinuousSocketProducerStrategy.class);

	
	public ContinuousSocketProducerStrategy(String filePath, boolean timestamped) {
		super(filePath,timestamped);
	}
	
	public  void createMessages(long startTime) throws Exception {
		try {
			
			ValueFactory valueFactory = new ValueFactory();
			long numberOfElements = 0;
			long delay = 0;
			
			String filePath = "input/raspberrypi.txt";
			BufferedReader reader = null;
			InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
			if (is == null) {
				throw new Exception("File Not Found: " + filePath);
			}
			reader = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line=reader.readLine()) != null) {
				if (line.startsWith("#") || Utils.isBlank(line)) {
					continue;
				}
				String[] tokens = line.split(",");
				if (tokens[0].equals("numberOfElements")) {
					numberOfElements = getValue(tokens);
					valueFactory.setNumberOfElements(numberOfElements);
				} else if (tokens[0].equals("delay")) {
					delay = getValue(tokens);
					valueFactory.setDelay(delay);
				} else if (tokens[0].equals("key")) {
					valueFactory.setKey(tokens[1]);
				} else if (tokens[0].equals("numberOfKeys")) {
					valueFactory.setNumberOfKeys(getValue(tokens));
				} else {
					valueFactory.createValueGenerator(tokens);
				}
			}
			close(reader);
			
			// Generate Values
			logger.info("====== GENERATE MESSAGES BEGIN");
			String msg = null;
			while ((msg = valueFactory.getNextMessage()) != null) {
				// logger.info("MSG=" + msg);
				System.out.println("MSG=" + msg);
				sendMessage(msg, valueFactory.getDelayValue());
			}
			logger.info("====== GENERATE MESSAGES END");
			
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	private long getValue(String[] tokens) {
		long value = Long.valueOf(tokens[1]);
		return value;
	}
	
	
	
	
//	public  void createMessages(long startTime) throws Exception {
//		BufferedReader reader = null;
//		logger.info("createMessages BEGIN");
//		logger.info("startTime=" + Utils.getFormattedTimestamp(startTime));
//		InputStream is = getClass().getClassLoader().getResourceAsStream(getFilePath());
//		if (is == null) {
//			throw new Exception("File Not Found: " + getFilePath());
//		}
//		reader = new BufferedReader(new InputStreamReader(is));
//		String line = reader.readLine();
//		String[] tokens = line.split(",");
//		int numberOfMessages = Integer.valueOf(tokens[0]);
//		int sleepDuration = Integer.valueOf(tokens[1]);
//		String msgData = getMessage(tokens);
//		close(reader);
//		for (int count = 0; count < numberOfMessages; count++) {
//			sendMessage(msgData,sleepDuration);
//		}
//		logger.info("createMessages END");
//	}
	

	private String getMessage(String[] tokens) {
		StringBuilder buffer = new StringBuilder();
		String sep = "";
		for (int i = 2; i < tokens.length; i++) {
			buffer.append(sep+tokens[i]);
			sep=",";
		}
		return buffer.toString();
	}
	
	
	
}
