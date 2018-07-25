package com.rueggerllc.flink.stream.beans;

public class EventBean {

	private long timestamp;
	private String key;
	private String label;
	private String value;
	private int processTimeOrder;
	private int eventTimeDelay;
	// private int processTimeDelay;
	private double processTimeDelay;
	
//	public String toString() {
//		StringBuilder buffer = new StringBuilder();
//		buffer.append("EventBean.key:" + key);
//		buffer.append("\nEventBean.label:" + label);
//		buffer.append("\nEventBean.value:" + value);
//		buffer.append("\nEventBean.processTimeOrder:" + processTimeOrder);
//		buffer.append("\nEventBean.eventTimeDelay:" + eventTimeDelay);
//		buffer.append("\nEventBean.processTimeDelay:" + processTimeDelay);
//		return buffer.toString();
//	}
	
	public String toString() {
		String line = String.format("%s %s %s %d %d %.2f",key,label,value,processTimeOrder,eventTimeDelay,processTimeDelay);
		return line;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public int getProcessTimeOrder() {
		return processTimeOrder;
	}
	public void setProcessTimeOrder(int processTimeOrder) {
		this.processTimeOrder = processTimeOrder;
	}
	public int getEventTimeDelay() {
		return eventTimeDelay;
	}
	public void setEventTimeDelay(int eventTimeDelay) {
		this.eventTimeDelay = eventTimeDelay;
	}

	public double getProcessTimeDelay() {
		return processTimeDelay;
	}

	public void setProcessTimeDelay(double processTimeDelay) {
		this.processTimeDelay = processTimeDelay;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	
	
}
