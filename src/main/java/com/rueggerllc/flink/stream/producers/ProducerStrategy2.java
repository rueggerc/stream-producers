package com.rueggerllc.flink.stream.producers;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

public class ProducerStrategy2 implements ProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ProducerStrategy2.class);
	private SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	private boolean stop = false;
	private boolean simulateDelay = false;
	private long startTime;
	
	public ProducerStrategy2(boolean simulateDelay) {
		this.simulateDelay = simulateDelay;
	}
	
	public void shutdown() {
		stop = true;
	}
	
	public void execute(PrintWriter socketWriter) throws Exception {
		if (simulateDelay == false) {
			executeNoDelay(socketWriter);
		} else {
			executeSimulateDelay(socketWriter);
		}
	}
	
	public void executeNoDelay(PrintWriter socketWriter) throws Exception {
		// Produce Sensor Events:
		// Time 13 sensor1, sensor1
		// Time 16 sensor1
		
		startTime = getTimestamp();
		
		// Period 1
		Thread.sleep(13000);
		long timestamp = getTimestamp();
		sendMessage(socketWriter,timestamp);
		sendMessage(socketWriter,timestamp);
		
		Thread.sleep(3000);
		timestamp = getTimestamp();
		sendMessage(socketWriter, timestamp);
	}
	
	public void executeSimulateDelay(PrintWriter socketWriter) throws Exception {
		// Produce Sensor Events:
		// Time 13 sensor1, eventTime=13
		// Time 16 sensor1, eventTime=16
		// Time 19 sensor1, eventTime=13
		
		startTime = getTimestamp();
		
		// Time 13
		Thread.sleep(13000);
		long timestamp13 = getTimestamp();
		sendMessage(socketWriter,timestamp13);
		
		// Time 16
		System.out.println("Sleep3");
		Thread.sleep(3000);
		long timestamp16 = getTimestamp();
		sendMessage(socketWriter, timestamp16);
		
		// Time 19
		// Simulate Delay
		System.out.println("Sleep3");
		Thread.sleep(3000);
		sendMessage(socketWriter, timestamp13);
		
	}
	
	
	
	private long getTimestamp() {
		long timestamp = Calendar.getInstance().getTimeInMillis();
		return timestamp;
	}
	
	private Date getDate(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		return calendar.getTime();
	}
	
	
	private void sendMessage(PrintWriter socketWriter, long timestamp) {
		String key = "sensor1";
		long delta = (getTimestamp() - startTime)/1000;
		Date timestampH = getDate(timestamp);
		String msg = String.format("id=%s timestamp=%d timestamph=%s t=%d", key, timestamp, format.format(timestampH), delta);
		System.out.println(msg);
		socketWriter.println(msg);
		socketWriter.flush();
	}
	
}
