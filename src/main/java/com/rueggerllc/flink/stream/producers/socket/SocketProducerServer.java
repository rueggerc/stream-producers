package com.rueggerllc.flink.stream.producers.socket;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.rueggerllc.flink.stream.producers.ProducerStrategy;

public class SocketProducerServer {

	private static Logger logger = Logger.getLogger(SocketProducerServer.class);
	private SocketProducerStrategy strategy;
	
	public SocketProducerServer(SocketProducerStrategy strategy) {
		this.strategy = strategy;
	}
	
	public void execute() {
			
			Scanner stdin = null;
			try {
				
				logger.info("SensorSocketServer Startup...");
				int portNumber = 9999;
				ServerSocket serverSocket = new ServerSocket(portNumber);
				logger.info("Waiting for Client...");
				
				Socket clientSocket = serverSocket.accept();
				logger.info("Got client connection");
				PrintWriter socketOutput = new PrintWriter(clientSocket.getOutputStream(), true);
				BufferedReader socketInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				strategy.setSocketWriter(socketOutput);
				
				// Start Data Producing Thread
				ProducerThread producer = new ProducerThread(strategy);
				producer.start();
				
				// Main Thread
				// Wait for user to indicate "exit" to shutdown
				logger.info("Type exit to shutdown");
				stdin = new Scanner(System.in);
				String line = "";
				while (!line.equalsIgnoreCase("exit") && !line.equalsIgnoreCase("quit")) {
					line = stdin.next();
				}
				
				logger.info("Stopping Producer...");
				producer.shutdown();
				
				// Exit
				producer.join();
				logger.info("CarSensorSocketServer Shutdown");
			
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}

	
	private static class ProducerThread extends Thread {
		
		private ProducerStrategy strategy;
		private boolean stop = false;

		
		public ProducerThread(ProducerStrategy strategy) {
			this.strategy = strategy;
		}
		
		
		public void run() {
			try {
				strategy.execute();
			} catch (Exception e) {
				logger.error("ERROR", e);
			} finally {
			}
		}	

		private void shutdown() {
			strategy.shutdown();
		}
		
	};
	
	
	public static void main(String[] args) {
		Scanner stdin = null;
		try {
			
		} catch (Exception e) {
			logger.error("Error", e);
		} finally {
			// stdin.close();
		}
	}
	
	

}
