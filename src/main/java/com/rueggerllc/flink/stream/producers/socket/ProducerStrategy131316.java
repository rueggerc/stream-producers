package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;

import org.apache.log4j.Logger;

public class ProducerStrategy131316 extends SocketProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ProducerStrategy131316.class);

	public void createMessages(PrintWriter socketWriter) throws Exception {
		
		// Produce Sensor Events:
		// Time 13 sensor1, sensor1
		// Time 16 sensor1
		
		// Period 1
		Thread.sleep(13000);
		long timestamp = getTimestamp();
		sendMessage(socketWriter,timestamp);
		sendMessage(socketWriter,timestamp);
		
		Thread.sleep(3000);
		timestamp = getTimestamp();
		sendMessage(socketWriter, timestamp);
	}
	
	public void shutdown() {
	}

}
