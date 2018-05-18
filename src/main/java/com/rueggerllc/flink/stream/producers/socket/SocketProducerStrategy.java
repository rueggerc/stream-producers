package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.producers.ProducerStrategy;
import com.rueggerllc.flink.stream.util.Utils;

public abstract class SocketProducerStrategy implements ProducerStrategy {
	
	private static Logger logger = Logger.getLogger(SocketProducerStrategy.class);
	private PrintWriter socketWriter;
	private long startTime;
	
	protected SocketProducerStrategy() {
	}
	
	
	public abstract void createMessages() throws Exception;
	
	protected long getTimestamp() {
		long timestamp = Calendar.getInstance().getTimeInMillis();
		return timestamp;
	}
	
	
	public void execute() throws Exception {
		startTime = getTimestamp();
		createMessages();
	}
	
	protected void sendMessage(String key, long timestamp) {
		long delta = (getTimestamp() - startTime)/1000;
		String msg = String.format("id=%s timestamp=%d timestamph=%s t=%d", key, timestamp, Utils.getFormattedTimestamp(timestamp), delta);
		System.out.println(msg);
		socketWriter.println(msg);
		socketWriter.flush();
	}


	public PrintWriter getSocketWriter() {
		return socketWriter;
	}


	public void setSocketWriter(PrintWriter socketWriter) {
		this.socketWriter = socketWriter;
	}
	
//	protected void sendMessage(PrintWriter socketWriter, long timestamp) {
//		String key = "sensor1";
//		long delta = (getTimestamp() - startTime)/1000;
//		Date timestampH = getDate(timestamp);
//		String msg = String.format("id=%s timestamp=%d timestamph=%s t=%d", key, timestamp, format.format(timestampH), delta);
//		System.out.println(msg);
//		socketWriter.println(msg);
//		socketWriter.flush();
//	}
	

	
	
}
