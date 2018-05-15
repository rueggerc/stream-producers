package com.rueggerllc.flink.stream.producers;

import java.io.PrintWriter;

public interface ProducerStrategy {
	
	public void execute(PrintWriter socketWriter) throws Exception;
	
	public void shutdown();
	
}
