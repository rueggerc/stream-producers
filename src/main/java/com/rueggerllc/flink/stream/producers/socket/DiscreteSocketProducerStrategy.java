package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;
import com.rueggerllc.flink.stream.util.Utils;


public class DiscreteSocketProducerStrategy extends SocketProducerStrategy {

	private static Logger logger = Logger.getLogger(DiscreteSocketProducerStrategy.class);
	
	
	public DiscreteSocketProducerStrategy(String filePath, boolean timestamped) throws Exception {
		super(filePath, timestamped);
	}
	
	protected  void createMessages() throws Exception {
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		InputStream is = getClass().getClassLoader().getResourceAsStream(getFilePath());
		if (is == null) {
			throw new Exception("File Not Found: " + getFilePath());
		}
		reader = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while ((line=reader.readLine()) != null) {
			if (Utils.isBlank(line) || line.trim().startsWith("#")) {
				continue;
			}
			if (getTimestamped()) {
				sendTimestampedMessage(line);
			} else {
				sendNoTimestampMessage(line);
			}
		}
		logger.info("createMessages END");
	}
	
	protected  void sendMessages() throws Exception {
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
		int sleepValue = Integer.valueOf(tokens[0]);
		sleep(sleepValue);
		long timestamp = getTimestamp(delta);
		msgData = String.format("%d,%s,%s", timestamp,msgData,getFormattedTimestamp(timestamp));
		sendMessage(msgData);		
	}
	
	protected void sendNoTimestampMessage(String line) {
		String[] tokens = line.split(",");
		int sleepValue = Integer.valueOf(tokens[0]);
		sleep(sleepValue);
		String msgData = getMessage(tokens);
		sendMessage(msgData);
	}
	
	
	

}
