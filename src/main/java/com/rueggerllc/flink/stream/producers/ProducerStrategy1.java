package com.rueggerllc.flink.stream.producers;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

public class ProducerStrategy1 implements ProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ProducerStrategy1.class);
	private SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	private boolean stop = false;
	
	public ProducerStrategy1() {
	}
	
	public void shutdown() {
		stop = true;
	}
	
	public void execute(PrintWriter socketWriter) throws Exception {
		
		// Produce Sensor Events:
		// Period 1 = 2
		// Period 2 = 3
		
		// Period 1
		logger.info("Period 1");
		Thread.sleep(1000);
		sendMessage(socketWriter, 0);
		Thread.sleep(1000);
		sendMessage(socketWriter, 0);
		Thread.sleep(13000);
		
		// Period 2
		logger.info("Period 2");
		Thread.sleep(1000);
		sendMessage(socketWriter, 0);
		Thread.sleep(1000);
		sendMessage(socketWriter, 0);
		Thread.sleep(1000);
		sendMessage(socketWriter, 0);
		Thread.sleep(12000);
		
	}
	
	
	private void sendMessage(PrintWriter socketWriter, int delay) {
		String key = "sensor1";
		Date now = Calendar.getInstance().getTime();
		long timestamp = Calendar.getInstance().getTimeInMillis();
		String msg = String.format("id=%s timestamp=%d timestamph=%s", key, timestamp, format.format(now));
		System.out.println(msg);
		socketWriter.println(msg);
	}
	
}
