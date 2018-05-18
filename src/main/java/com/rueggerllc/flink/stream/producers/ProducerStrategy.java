package com.rueggerllc.flink.stream.producers;

public interface ProducerStrategy {
	
	public void execute() throws Exception;
	
	public void shutdown();
	
}
