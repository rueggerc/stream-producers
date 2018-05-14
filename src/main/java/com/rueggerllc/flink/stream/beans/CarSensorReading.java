package com.rueggerllc.flink.stream.beans;

public class CarSensorReading {

	private String id;
	private String event;
	private long timestamp;
	
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("CarSensorReading.id: " + id);
		buffer.append("\nCarSensorReading.event: " + event);
		buffer.append("\nCarSensorReading.timestamp: " + timestamp);
		return buffer.toString();
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getEvent() {
		return event;
	}
	public void setEvent(String event) {
		this.event = event;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	
}
