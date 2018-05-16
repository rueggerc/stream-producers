package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.producers.ProducerStrategy;

public abstract class SocketProducerStrategy implements ProducerStrategy {
	
	private static Logger logger = Logger.getLogger(SocketProducerStrategy.class);
	private SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	private long startTime;
	
	public abstract void createMessages(PrintWriter socketWriter) throws Exception;
	
	protected long getTimestamp() {
		long timestamp = Calendar.getInstance().getTimeInMillis();
		return timestamp;
	}
	
	protected Date getDate(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		return calendar.getTime();
	}
	
	public void execute(PrintWriter socketWriter) throws Exception {
		startTime = getTimestamp();
		createMessages(socketWriter);
	}
	
	
	protected void sendMessage(PrintWriter socketWriter, long timestamp) {
		String key = "sensor1";
		long delta = (getTimestamp() - startTime)/1000;
		Date timestampH = getDate(timestamp);
		String msg = String.format("id=%s timestamp=%d timestamph=%s t=%d", key, timestamp, format.format(timestampH), delta);
		System.out.println(msg);
		socketWriter.println(msg);
		socketWriter.flush();
	}
	
	
}
