package com.rueggerllc.flink.tests;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.flink.stream.producers.kafka.MessageProducer;
import com.rueggerllc.flink.stream.util.Utils;


public class KafkaTests {

	private static Logger logger = Logger.getLogger(KafkaTests.class);
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092,oscar:9092";


	@BeforeClass
	public static void setupClass() throws Exception {
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}

	@Test
	@Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
		
	
	@Test
	// @Ignore
	public void testMessageProducer() {
		try {
	
	        String topicName = "number-topic";
	    	MessageProducer messageProducer = new MessageProducer();
	    	messageProducer.setTopicName(topicName);
	    	messageProducer.execute();
	    	
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}

	
	@Test
	@Ignore
	public void testCreateBadNumberStream() {
		try {
	
	        String topicName = "number-topic";
	    	
	        // Configure the Producer
	        Properties configProperties = new Properties();
	        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
	        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        Producer<String,String> producer = new KafkaProducer<>(configProperties);			
	        createNumberStream(producer, topicName);
	        
	        // Done
	        producer.close();
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	
    private void createNumberStream(Producer<String,String>  producer, String topicName) {
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
	
	
	@Test
	@Ignore
	public void testReadFile() {
		try {
			String filePath = "input/raspberrypi.txt";
			BufferedReader reader = null;
			InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
			if (is == null) {
				throw new Exception("File Not Found: " + filePath);
			}
			reader = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line=reader.readLine()) != null) {
				if (line.startsWith("#") || Utils.isBlank(line)) {
					continue;
				}
				logger.info(line);
			}
			close(reader);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	private void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}
	
	
	

}