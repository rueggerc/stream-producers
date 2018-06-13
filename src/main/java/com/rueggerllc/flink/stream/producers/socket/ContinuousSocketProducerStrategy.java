package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.util.Utils;

public class ContinuousSocketProducerStrategy extends SocketProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ContinuousSocketProducerStrategy.class);

	
	public ContinuousSocketProducerStrategy(String filePath, boolean timestamped) {
		super(filePath,timestamped);
	}
	
	
	public  void createMessages(long startTime) throws Exception {
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		logger.info("startTime=" + Utils.getFormattedTimestamp(startTime));
		InputStream is = getClass().getClassLoader().getResourceAsStream(getFilePath());
		if (is == null) {
			throw new Exception("File Not Found: " + getFilePath());
		}
		reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		String[] tokens = line.split(",");
		int numberOfMessages = Integer.valueOf(tokens[0]);
		int sleepDuration = Integer.valueOf(tokens[1]);
		String msgData = getMessage(tokens);
		close(reader);
		for (int count = 0; count < numberOfMessages; count++) {
			sendMessage(msgData,sleepDuration);
		}
		logger.info("createMessages END");
	}
	

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
