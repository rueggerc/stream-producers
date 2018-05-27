package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Calendar;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.producers.ProducerStrategy;
import com.rueggerllc.flink.stream.util.Utils;

public class SocketProducerStrategy implements ProducerStrategy {
	
	private static Logger logger = Logger.getLogger(SocketProducerStrategy.class);
	private boolean timestamped = false;
	private PrintWriter socketWriter;
	private String filePath;
	private long startTime;

	
	public SocketProducerStrategy(String filePath, boolean timestamped) {
		this.filePath = filePath;
		this.timestamped = timestamped;
	}
	
	
	public  void createMessages() throws Exception {
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
		if (is == null) {
			throw new Exception("File Not Found: " + filePath);
		}
		reader = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while ((line=reader.readLine()) != null) {
			if (line.startsWith("sleep")) {
				sleep(line);
			} else {
				sendMessage(line);
			}
		}
		logger.info("createMessages END");
	}
	
	
	
	private void sleep(String line) {
		try {
			String[] tokens = line.split(" ");
			int sleepValue = Integer.valueOf(tokens[1]);
			sleep(sleepValue);
		} catch (Exception e) {
			logger.error("ERROR",e);
		}
	}
	
	private void sleep(int sleepValue) {
		try {
			Thread.sleep(sleepValue*1000);
		} catch (Exception e) {
			logger.error("ERROR",e);
		}
	}
	
	private void sendMessage(String line) {
		if (timestamped == false) {
			socketWriter.println(line);
			socketWriter.flush();
		} else { 
			String[] tokens = line.split(" ");
			String key = tokens[0];
			int delayValue = Integer.valueOf(tokens[1]);
			sendMessage(key,getTimestamp(delayValue));
		}
	}
	
	private void sendTimetampedMessage(String line) {
		String[] tokens = line.split(" ");
		String key = tokens[0];
		int delayValue = Integer.valueOf(tokens[1]);
		sendMessage(key,getTimestamp(delayValue));
	}
	
	protected long getNow() {
		return Calendar.getInstance().getTimeInMillis();
	}
	protected long getTimestamp() {
		return Calendar.getInstance().getTimeInMillis();
	}
	protected long getTimestamp(int delayValue) {
		return Calendar.getInstance().getTimeInMillis() - (delayValue*1000);
	}
	
	
	public void execute() throws Exception {
		startTime = getTimestamp();
		createMessages();
	}
	
	protected void sendMessage(String key, long timestamp) {
		long delta = (getNow() - startTime)/1000;
		String msg = String.format("id=%s timestamp=%d timestamph=%s t=+%d", key, timestamp, Utils.getFormattedTimestamp(timestamp), delta);
		logger.info(msg);
		socketWriter.println(msg);
		socketWriter.flush();
	}


	public PrintWriter getSocketWriter() {
		return socketWriter;
	}


	public void setSocketWriter(PrintWriter socketWriter) {
		this.socketWriter = socketWriter;
	}
	

	public void shutdown() {
	}
	

	
	
}
