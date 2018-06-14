package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;
import java.io.Reader;
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
	}
	public void shutdown() {
		socketWriter.close();
	}
	
	
	protected abstract void createMessages(long startTime) throws Exception;
	
	
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
	
	protected void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}


	

	
	
}
