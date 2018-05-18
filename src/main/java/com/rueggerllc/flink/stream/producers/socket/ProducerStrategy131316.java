package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;

import org.apache.log4j.Logger;

public class ProducerStrategy131316 extends SocketProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ProducerStrategy131316.class);
	
	public ProducerStrategy131316() {
	}

	public void createMessages() throws Exception {
		
		// Produce Sensor Events:
		// Time 13 sensor1, sensor1
		// Time 16 sensor1
		
		// Period 1
		Thread.sleep(13000);
		long timestamp = getTimestamp();
		sendMessage("sensor1",timestamp);
		sendMessage("sensor1",timestamp);
		
		Thread.sleep(3000);
		timestamp = getTimestamp();
		sendMessage("sensor1", timestamp);
	}
	
	public void shutdown() {
	}

}
