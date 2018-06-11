package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;
import java.util.Calendar;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.producers.ProducerStrategy;
import com.rueggerllc.flink.stream.util.Utils;

public abstract class SocketProducerStrategy implements ProducerStrategy {
	
	private static Logger logger = Logger.getLogger(SocketProducerStrategy.class);
	private boolean timestamped = false;
	private PrintWriter socketWriter;
	private String filePath;
	private long startTime;

	
	public SocketProducerStrategy(String filePath, boolean timestamped) {
		this.filePath = filePath;
		this.timestamped = timestamped;
	}
	
	public void execute() throws Exception {
		startTime = getNow();
		createMessages(startTime);
		socketWriter.close();
	}
	public void shutdown() {
	}
	
	
	protected abstract void createMessages(long startTime) throws Exception;
	

//	protected void sendTimestampedMessage(String line) {
//		String[] tokens = line.split(",");
//		int sleepValue = Integer.valueOf(tokens[0]);
//		sleep(sleepValue);
//		int delta = Integer.valueOf(tokens[1]);
//		String msgData = getMessage(tokens);
//		long timestamp = getTimestamp(delta);
//		String msg = String.format("%d,%s", timestamp, msgData);
//		String msgDebug = String.format("%d,%s TS=(%s)", timestamp, msgData, Utils.getFormattedTimestamp(timestamp));
//		socketWriter.println(msg);
//		socketWriter.flush();
//		logger.debug(msgDebug);
//	}
	
//	protected String getMessage(String[] tokens) {
//		StringBuilder buffer = new StringBuilder();
//		String sep = "";
//		for (int i = 2; i < tokens.length; i++) {
//			buffer.append(sep+tokens[i]);
//			sep=",";
//		}
//		return buffer.toString();
//	}
	
	
	protected void sleep(int sleepValue) {
		try {
			if (sleepValue == 0) {
				return;
			}
			Thread.sleep(sleepValue*1000);
		} catch (Exception e) {
			logger.error("ERROR",e);
		}
	}
	
	protected void sendMessage(String msg, int sleepValue) {
		sleep(sleepValue);
		socketWriter.println(msg);
		socketWriter.flush();
		logger.debug(msg);
	}
	
	
	protected long getNow() {
		return Calendar.getInstance().getTimeInMillis();
	}
	protected long getTimestamp(int delta) {
		return Calendar.getInstance().getTimeInMillis() - (delta*1000);
	}
	
	
	protected PrintWriter getSocketWriter() {
		return socketWriter;
	}


	public void setSocketWriter(PrintWriter socketWriter) {
		this.socketWriter = socketWriter;
	}
	
	protected String getFilePath() {
		return filePath;
	}
	protected boolean getTimestamped() {
		return timestamped;
	}


	

	
	
}
