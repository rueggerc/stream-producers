package com.rueggerllc.flink.stream.producers.kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


public class ProducerApp {
	
	private static final Logger logger = Logger.getLogger(ProducerApp.class);
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092,oscar:9092";
    
    public static void main(String[] argv)throws Exception {
    	
    	try {
//	        if (argv.length != 1) {
//	            logger.error("Please specify Topic Name");
//	            System.exit(-1);
//	        }
	        // String topicName = argv[0];
	        String topicName = "number-topic";
	
	        // Configure the Producer
	        Properties configProperties = new Properties();
	        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
	        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        Producer<String,String> producer = new KafkaProducer<>(configProperties);

//	        for (int i = 0; i < 10; i++) {
//	        	long now = System.currentTimeMillis();
//	        	 String line = String.format("More Kafka Data: " + now + " " + new Date(now));
//	             ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, "key1", line);
//	             producer.send(rec);
//	             System.out.println(line);
//	        }
	        
	        createNumberStream(producer, topicName);
	        
	        producer.close();
    	} catch (Exception e) {
        	System.out.println("ERROR:\n" + e);
        }
    }
    
    private static void createNumberStream(Producer<String,String>  producer, String topicName) {
    	String line = String.format("set1,%s", "3");
    	ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
    	System.out.println(line);
    	
    	line = String.format("set1,%s", "8");
    	msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
    	System.out.println(line);
    	
    	line = String.format("set1,%s", "4");
    	msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
    	System.out.println(line);
    	
    	line = String.format("set1,%s", "foobar");
    	msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
    	System.out.println(line);
    	
    	line = String.format("set1,%s", "9");
    	msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
    	System.out.println(line);
    	
       	line = String.format("set1,%s", "6");
    	msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
    	System.out.println(line);
    	
       	line = String.format("set1,%s", "12");
    	msg = new ProducerRecord<String, String>(topicName, "key1", line);
    	producer.send(msg);
    	System.out.println(line);
    	
    	
    }
    
    
}
