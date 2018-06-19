package com.rueggerllc.flink.stream.producers.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


public class MessageProducer {
	
	private static final Logger logger = Logger.getLogger(MessageProducer.class);
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092,oscar:9092";
	private Producer<String,String> producer = null;
	private String topicName;
	
	public void execute() {
		
		logger.info("Message Producer Startup...");
		if (topicName == null || topicName.trim().equals("")) {
			logger.error("Topic Name Not Set");
			return;
		}
		
        // Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        
        producer = new KafkaProducer<>(configProperties);
        
		logger.info("Type exit to shutdown");
		Scanner stdin = new Scanner(System.in);
		while (true) {
			String line = getLine(stdin);
			if (line.equals("quit")) {
				break;
			}
			sendMessage(line);
		}
		producer.close();
		logger.info("Message Producer Shutdown...");
		
	}
	
	private String getLine(Scanner stdin) {
		String line = stdin.next();
		if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
			return "quit";
		}
		return line;
	}
	
	private void sendMessage(String line) {
    	ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
    
    
    
}
