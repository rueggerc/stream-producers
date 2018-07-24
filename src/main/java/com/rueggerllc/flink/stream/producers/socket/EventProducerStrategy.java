package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.beans.EventBean;
import com.rueggerllc.flink.stream.util.EventComparator;
import com.rueggerllc.flink.stream.util.Utils;


public class EventProducerStrategy extends SocketProducerStrategy {

	private static Logger logger = Logger.getLogger(EventProducerStrategy.class);
	private List<EventBean> eventBeans;
	
	public EventProducerStrategy(String filePath, boolean timestamped) throws Exception {
		super(filePath, timestamped);
	}
	
	protected  void createMessages() throws Exception {
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		InputStream is = getClass().getClassLoader().getResourceAsStream(getFilePath());
		if (is == null) {
			throw new Exception("File Not Found: " + getFilePath());
		}
		
		reader = new BufferedReader(new InputStreamReader(is));
		String line = null;
		eventBeans = new ArrayList<>();
		while ((line=reader.readLine()) != null) {
			if (line.startsWith("#")) {
				continue;
			}

			System.out.println(line);
			String[] tokens = line.split(",");
			String key = tokens[0];
			String label = tokens[1];
			String value = tokens[2];
			int processTimeOrder = Integer.parseInt(tokens[3]);
			int eventTimeDelay = Integer.parseInt(tokens[4]);
			int processTimeDelay = Integer.parseInt(tokens[5]);
			
			EventBean event = new EventBean();
			event.setKey(key);
			event.setLabel(label);
			event.setValue(value);
			event.setProcessTimeOrder(processTimeOrder);
			event.setEventTimeDelay(eventTimeDelay);
			event.setProcessTimeDelay(processTimeDelay);
			eventBeans.add(event);
		}
		close(reader);
		logger.info("createMessages END");
	}
	
	protected  void sendMessages() throws Exception {
		logger.info("sendMessages BEGIN");
		
		// Set Event Times
		long eventTime = System.currentTimeMillis();
		for (EventBean eventBean : eventBeans) {
			eventTime += eventBean.getEventTimeDelay()*1000;
			eventBean.setTimestamp(eventTime);
		}

		// Send messages in Process Time Order
		Collections.sort(eventBeans, new EventComparator());
		long startTime = System.currentTimeMillis();
		for (EventBean eventBean : eventBeans) {
			StringBuilder buffer = new StringBuilder();
			buffer.append(eventBean.getKey()+",");
			buffer.append(eventBean.getLabel()+",");
			buffer.append(eventBean.getValue()+",");
			buffer.append(eventBean.getTimestamp()+",");
			buffer.append(Utils.getFormattedTimestamp(eventBean.getTimestamp()));
			String message = buffer.toString();
			System.out.println(message);
			int delay = eventBean.getEventTimeDelay() + eventBean.getProcessTimeDelay();
			sendMessage(message, delay);
		}
		logger.info("sendMessages END");
	}
	

	

}
