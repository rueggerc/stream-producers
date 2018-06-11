package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;
import com.rueggerllc.flink.stream.util.Utils;


public class DiscreteSocketProducerStrategy extends SocketProducerStrategy {

	private static Logger logger = Logger.getLogger(DiscreteSocketProducerStrategy.class);
	
	
	public DiscreteSocketProducerStrategy(String filePath, boolean timestamped) {
		super(filePath, timestamped);
	}
	
	protected  void createMessages(long startTime) throws Exception {
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		logger.info("startTime=" + Utils.getFormattedTimestamp(startTime));
		InputStream is = getClass().getClassLoader().getResourceAsStream(getFilePath());
		if (is == null) {
			throw new Exception("File Not Found: " + getFilePath());
		}
		reader = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while ((line=reader.readLine()) != null) {
			if (getTimestamped()) {
				sendTimestampedMessage(line);
			} else {
				sendMessage(line);
			}
		}
		logger.info("createMessages END");
	}
	
	protected String getMessage(String[] tokens) {
		StringBuilder buffer = new StringBuilder();
		String sep = "";
		for (int i = 2; i < tokens.length; i++) {
			buffer.append(sep+tokens[i]);
			sep=",";
		}
		return buffer.toString();
	}
	
	
	protected void sendTimestampedMessage(String line) {
		String[] tokens = line.split(",");
		String msgData = getMessage(tokens);
		int delta = Integer.valueOf(tokens[1]);
		long timestamp = getTimestamp(delta);
		int sleepValue = Integer.valueOf(tokens[0]);
		msgData = String.format("%d,%s", timestamp, msgData);
		sendMessage(msgData,sleepValue);		
		String msgDebug = String.format("%d,%s TS=(%s)", timestamp, msgData, Utils.getFormattedTimestamp(timestamp));
		logger.debug(msgDebug);

	}
	
	protected void sendMessage(String line) {
		String[] tokens = line.split(",");
		int sleepValue = Integer.valueOf(tokens[0]);
		String msgData = getMessage(tokens);
		sendMessage(msgData, sleepValue);
	}
	
	
	

}
