package ulb.cs.dsa.streams;

import ulb.cs.dsa.streams.MapLargeFile;

public class CallThread extends Thread {
	/**
	 * K:-The number of I/O streams
	 */
	final static int K = 1;
	
	private String filename = null;
	public CallThread(String string) {
		super();
		filename = string;
		start();
	}

	

	public void run() {
		try {
			
			new MapLargeFile(filename);
		} catch (Exception e1) {
			
			e1.printStackTrace();
		}
		
	}

	public static void main(String[] args) {
		
		for (int i = 0; i < 50; i++)
			new CallThread("File"+i+".dat");
	}
}