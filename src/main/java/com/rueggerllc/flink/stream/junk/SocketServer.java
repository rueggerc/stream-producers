package com.rueggerllc.flink.stream.junk;

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

public class SocketServer {

	private static Logger logger = Logger.getLogger(SocketServer.class);
	
	public static void main(String[] args) {
		
		Scanner stdin = null;
		try {
			
			logger.info("SocketServer Startup...");
			int portNumber = 9999;
			ServerSocket serverSocket = new ServerSocket(portNumber);
			logger.info("Wating for Client...");
			
			Socket clientSocket = serverSocket.accept();
			logger.info("Got client connection");
			PrintWriter socketOutput = new PrintWriter(clientSocket.getOutputStream(), true);
			BufferedReader socketInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			
			// Start Data Producing Thread
			ProducerThread producer = new ProducerThread(socketOutput);
			producer.start();
			
			// Main Thread
			// Wait for user to indicate "exit" to shutdown
			stdin = new Scanner(System.in);
			String line = "";
			while (!line.equals("exit")) {
				line = stdin.next();
			}
			
			logger.info("Stopping Producer...");
			producer.shutdown();
			
			// Exit
			producer.join();
			logger.info("SocketServer Shutdown");
			
			
		} catch (Exception e) {
			logger.error("Error", e);
		} finally {
			stdin.close();
		}
	}
	
	private static class ProducerThread extends Thread {
		
		private boolean stop = false;
		private PrintWriter socketWriter;
		private SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		private int tempCount = 0;
		private int humidityCount=56;
		private int humidityBase = 1000;
		
		public ProducerThread(PrintWriter socketWriter) {
			this.socketWriter = socketWriter;
		}
		
		public void run() {
			try {
				while (stop == false) {
					String temp = computeTemp();
					socketWriter.println(temp);
					String humidity = computeHumidity();
					socketWriter.println(humidity);
					sleep(60000);
				}
			} catch (Exception e) {
				logger.error("ERROR", e);
			} finally {
			}
		}
		
		private String computeTemp() {
			String key = "temp";
			Date now = Calendar.getInstance().getTime();
			String line = String.format("id=%s temperature=%d timestamp=%s", key, (tempCount++%10), format.format(now));
			return line;
		}
		private String computeHumidity() {
			String key = "humidity";
			Date now = Calendar.getInstance().getTime();
			String line = String.format("id=%s humidity=%d timestamp=%s", key, (humidityCount++%8)+humidityBase, format.format(now));
			return line;
		}
		
		private void shutdown() {
			stop = true;
		}
		
	};
	
	

}
