package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.beans.EventBean;
import com.rueggerllc.flink.stream.util.Utils;


public class EventProducerStrategy1 extends SocketProducerStrategy {

	private static Logger logger = Logger.getLogger(EventProducerStrategy1.class);
	private List<EventBean> eventBeans;
	
	public EventProducerStrategy1(String filePath) throws Exception {
		super(filePath);
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
			if (line.startsWith("#") || Utils.isBlank(line.trim())) {
				continue;
			}

			String[] tokens = line.split(",");
			String key = tokens[0];
			String label = tokens[1];
			String value = tokens[2];
			double eventTimeDelay = Double.parseDouble(tokens[3]);
			double processTimeDelay = Double.parseDouble(tokens[4]);
			
			EventBean eventBean = new EventBean();
			eventBean.setKey(key);
			eventBean.setLabel(label);
			eventBean.setValue(value);
			eventBean.setEventTimeDelay(eventTimeDelay);
			eventBean.setProcessTimeDelay(processTimeDelay);
			eventBeans.add(eventBean);
		}
		close(reader);
		logger.info("createMessages END");
	}
	
	protected  void sendMessages() throws Exception {
		logger.info("sendMessages BEGIN");
		// Send messages
		for (EventBean eventBean : eventBeans) {
			double eventTimeDelay = eventBean.getEventTimeDelay();
			sleep(eventTimeDelay);
			eventBean.setTimestamp(getNow());
			long processTimeDelay = (long)(eventBean.getProcessTimeDelay()*1000);
			if (processTimeDelay == 0) {
				sendMessage(eventBean.toMessage());
			} else {
				EventTimerTask timerTask = new EventTimerTask(this, eventBean);
				Timer timer = new Timer("EventTimer");
				timer.schedule(timerTask, processTimeDelay);
			}
		}
		logger.info("sendMessages END");
	}
	
	private static class EventTimerTask extends TimerTask {
		private SocketProducerStrategy strategy;
		private EventBean eventBean;
		
		public EventTimerTask(SocketProducerStrategy strategy, EventBean eventBean) {
			this.strategy = strategy;
			this.eventBean = eventBean;
		}

		@Override
		public void run() {
			strategy.sendMessage(eventBean.toMessage());
		}
		
		
	}
	

	

}
