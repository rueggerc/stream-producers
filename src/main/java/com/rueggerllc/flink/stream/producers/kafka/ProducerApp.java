package com.rueggerllc.flink.stream.producers.kafka;

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
	        if (argv.length != 1) {
	            logger.error("Please specify Topic Name");
	            System.exit(-1);
	        }
	        String topicName = argv[0];
	  
	
	        // Configure the Producer
	        Properties configProperties = new Properties();
	        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
	        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        Producer<String,String> producer = new KafkaProducer<>(configProperties);

	        // Message1
	        String line = String.format("%s,%d","set1",3);
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, "key1", line);
            producer.send(rec);
            
	        // Message2
	        line = String.format("%s,%d","set1",8);
            rec = new ProducerRecord<String, String>(topicName,"key2",line);
            producer.send(rec);
            
	        // Message3
	        line = String.format("%s,%d","set1",4);
            rec = new ProducerRecord<String, String>(topicName,"key3",line);
            producer.send(rec);   
            
	        // Message4
	        line = String.format("%s,%s","set1","foobar");
	        rec = new ProducerRecord<String, String>(topicName,"key4",line);
            producer.send(rec);   
            
	        // Message5
	        line = String.format("%s,%d","set1",9);
	        rec = new ProducerRecord<String, String>(topicName,"key5",line);
            producer.send(rec);   
	        
	        // Message6
	        line = String.format("%s,%d","set1",6);
	        rec = new ProducerRecord<String, String>(topicName,"key6",line);
            producer.send(rec);   
            
	        // Message7
	        line = String.format("%s,%d","set1",12);
	        rec = new ProducerRecord<String, String>(topicName,"key7",line);
            producer.send(rec);   
            
	        producer.close();
    	} catch (Exception e) {
        	System.out.println("ERROR:\n" + e);
        }
    }
}
