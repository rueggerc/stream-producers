package com.rueggerllc.flink.stream.junk;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class ClientNC {
	
	public static void main(String args[]) {
		
		DataInputStream is;
		DataOutputStream os;
		boolean result = true;
		try {		
			
			Socket socket = new Socket(InetAddress.getByName("localhost"), 9999);
			
			is = new DataInputStream(socket.getInputStream());
			os = new DataOutputStream(socket.getOutputStream());
			PrintWriter pw = new PrintWriter(os);
			
			for (int i=0; i < 10; i++) {
				String string = "Here is the data: " + i;
				pw.println(string);
			}
			
			pw.flush();
			
			// is.close();
			// os.close();
			
		} catch (Exception e) {
			System.out.println("ERROR:" + e);
		}
	}

}
