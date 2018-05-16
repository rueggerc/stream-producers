package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;

import org.apache.log4j.Logger;

public class ProducerStrategy131619 extends SocketProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ProducerStrategy131619.class);

	public void shutdown() {
	}
	
	
	public void createMessages(PrintWriter socketWriter) throws Exception {
		// Produce Sensor Events:
		// Time 13 sensor1, eventTime=13
		// Time 16 sensor1, eventTime=16
		// Time 19 sensor1, eventTime=13
		
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
	
}
