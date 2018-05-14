package com.rueggerllc.flink.stream.producers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

import org.apache.log4j.Logger;

public class CarSensorSocketServer {

	private static Logger logger = Logger.getLogger(CarSensorSocketServer.class);
	private static int generationPeriodSeconds = 60;
	
	public static void main(String[] args) {
		
		
		Scanner stdin = null;
		try {
			
			logger.info("CarSensorSocketServer Startup...");
			int portNumber = 9999;
			ServerSocket serverSocket = new ServerSocket(portNumber);
			logger.info("Waiting for Client...");
			
			Socket clientSocket = serverSocket.accept();
			logger.info("Got client connection");
			PrintWriter socketOutput = new PrintWriter(clientSocket.getOutputStream(), true);
			BufferedReader socketInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			
			// Start Data Producing Thread
			ProducerThread producer = new ProducerThread(socketOutput, generationPeriodSeconds);
			producer.start();
			
			// Main Thread
			// Wait for user to indicate "exit" to shutdown
			logger.info("Type exit to shutdown");
			stdin = new Scanner(System.in);
			String line = "";
			while (!line.equals("exit")) {
				line = stdin.next();
			}
			
			logger.info("Stopping Producer...");
			producer.shutdown();
			
			// Exit
			producer.join();
			logger.info("CarSensorSocketServer Shutdown");
			
			
		} catch (Exception e) {
			logger.error("Error", e);
		} finally {
			// stdin.close();
		}
	}
	
	private static class ProducerThread extends Thread {
		
		private boolean stop = false;
		private PrintWriter socketWriter;
		private SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		private int period = 0;
		private int generationPeriodSeconds = 0;

		
		public ProducerThread(PrintWriter socketWriter, int generationPeriodSeconds) {
			this.socketWriter = socketWriter;
			this.generationPeriodSeconds = generationPeriodSeconds;
		}
		
		
		public void run() {
			try {
				
				logger.info("CarSensor Use Case BEGIN");
				
				senseCars(2);
				senseCars(3);
				senseCars(1);
				senseCars(2);
				
//				senseCars(4);
//				senseCars(8);
//				senseCars(3);
//				senseCars(7);
//				
//				senseCars(4);
//				senseCars(8);
//				senseCars(6);
//				senseCars(9);
				
				logger.info("CarSensor Use Case END");
				
				
			} catch (Exception e) {
				logger.error("ERROR", e);
			} finally {
			}
		}
		
		private void senseCars(int numCars) throws Exception {
			
			String key = "sensor1";
			// int sleepAmount = 60000/numCars;
			// int sleepAmount = 15000/numCars;
			
			// Number of cars every 10 seconds
			int generationPeriod = generationPeriodSeconds * 1000;
			int sleepAmount = generationPeriod/numCars;
			for (int i = 0; i < numCars && stop==false; i++) {
				Date now = Calendar.getInstance().getTime();
				
				long milliseconds = Calendar.getInstance().getTimeInMillis();
				long seconds = java.time.Instant.now().getEpochSecond();
				
				String line = String.format("id=%s event=car period=%d timestamp=%d timestamph=%s", key, period, milliseconds, format.format(now));
				System.out.println(line);
				socketWriter.println(line);
				System.out.println("Sleep=" + sleepAmount);
				sleep(sleepAmount);
			}
			period++;
		}
		

		private void shutdown() {
			stop = true;
		}
		
	};
	
	

}
