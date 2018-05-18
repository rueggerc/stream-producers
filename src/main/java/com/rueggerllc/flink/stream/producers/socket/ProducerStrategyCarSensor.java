package com.rueggerllc.flink.stream.producers.socket;

import java.io.PrintWriter;

import org.apache.log4j.Logger;

public class ProducerStrategyCarSensor extends SocketProducerStrategy {
	
	private static Logger logger = Logger.getLogger(ProducerStrategyCarSensor.class);
	
	public ProducerStrategyCarSensor() {
	}

	public void shutdown() {
	}
	
	
	public void createMessages() throws Exception {
		
		// Produce Car Sensor Events
		
		// Period1: 2 Events
		Thread.sleep(10000/2);
		long timestamp = getTimestamp();
		sendMessage("sensor1", timestamp);
		sendMessage("sensor1", timestamp);
		Thread.sleep(10000/2);
		
		// Period2: 3 Events
		Thread.sleep(10000/2);
		timestamp = getTimestamp();
		sendMessage("sensor1", timestamp);
		sendMessage("sensor1", timestamp);
		sendMessage("sensor1", timestamp);
		Thread.sleep(10000/2);
		
		// Period3: 1 Events
		Thread.sleep(10000/2);
		timestamp = getTimestamp();
		sendMessage("sensor1", timestamp);
		Thread.sleep(10000/2);
		
		// Period4: 2 Events
		Thread.sleep(10000/2);
		timestamp = getTimestamp();
		sendMessage("sensor1", timestamp);
		sendMessage("sensor1", timestamp);
		
		

//		senseCars(2);
//		senseCars(3);
//		senseCars(1);
//		senseCars(2);
		
//		senseCars(4);
//		senseCars(8);
//		senseCars(3);
//		senseCars(7);
//		
//		senseCars(4);
//		senseCars(8);
//		senseCars(6);
//		senseCars(9);
		
		
		
	}
	
}
