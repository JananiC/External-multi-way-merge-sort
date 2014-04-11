/****************************************************************
 * Class: DataIOStream.java
 * Description: This class demonstrates the reading and writing 
 * 				operations using DataInput and DataOutput 
 * 				streams
 * Date: 11/05/2012
 * @author JANANI CHAKKARADHARI, STEPHANY GARCIA
 *
 ***************************************************************/

package ulb.cs.dsa.streams;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;

public class DataIOStream {
	/**
	 * N:-The input data for writing
	 */
	final static int N = 10000;
	/**
	 * K:-The number of I/O streams
	 */
	final static int K = 25;
	/**
	 * The location of input (to read)/output(to write) files
	 */
	final static String INPUT_LARGE_FILE = "E://MMB_TEST//Largefile//";
	/**
	 * The range of random numbers to be generated
	 */
	final static int RANDOM_RANGE = 1000;

	/**
	 * @param
	 */
	public static void main(String[] args) {
		DataIOStream dataIO = new DataIOStream();
		dataIO.writeDataFiles();
		dataIO.readDataFiles();

	}

	/**
	 * . Method to write data on "K" files using DataOutputStream
	 */
	public void writeDataFiles() {
		DataOutputStream dos = null;
		Iterator<OutputStream> it = null;
		OutputStream fileOut = null;
		try {
			/**
			 * Creating an ArrayList of OutputStream for holding K
			 * FileOutputStreams of Files
			 */
			ArrayList<OutputStream> outputStreamList = new ArrayList<OutputStream>();
			int i = 1;
			while (i <= K) {
				outputStreamList.add(new FileOutputStream(INPUT_LARGE_FILE
						+ "File" + i));
				// System.out.println("Opened file in write--" + i);
				i++;
			}

			Common com = new Common(RANDOM_RANGE);
			/**
			 * . Initializing the current time for measuring time for write
			 * operation
			 */
			long initialTime = System.currentTimeMillis();
			/**
			 * outer for loop to iterate for N times to write
			 */
			for (int j = 1; j <= N; j++) {
				// System.out.println("Writing " + j + " th element");
				int randomNum = com.getRandomNum();
				/**
				 * Iterate the outputStreamList to get each outputStream and
				 * wrap it with DataOutputStream and perform write
				 */
				it = outputStreamList.iterator();

				while (it.hasNext()) {
					/*
					 * System.out.println(" In " + streamCount + " file @@" +
					 * randomNum);
					 */
					fileOut = it.next();
					dos = new DataOutputStream(fileOut);
					dos.writeInt(randomNum);
				}

			}
			long timeToComplete = System.currentTimeMillis() - initialTime;
			System.out.println("Total time required to write the data file-->"
					+ timeToComplete + "ms");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			/**
			 * Closing all the files
			 */
			while (it.hasNext()) {
				try {
					dos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		}

	}

	/**
	 * . Method to read data on "K" files using DataInputStream
	 */
	public void readDataFiles() {

		Iterator<InputStream> it = null;
		InputStream fileIn = null;
		DataInputStream dataInputSt = null;
		try {
			/**
			 * Creating an ArrayList of InputStream for holding K
			 * FileInputStreams of Files
			 */
			ArrayList<InputStream> inputStreamList = new ArrayList<InputStream>();
			File folder = new File(INPUT_LARGE_FILE);
			File[] listOfFiles = folder.listFiles();
			int i = 1;
			while (i <= K) {
				inputStreamList.add(new FileInputStream(listOfFiles[i - 1]));
				// System.out.println("Opened file in read--" + i);
				i++;
			}
			/**
			 * . Initializing the current time for measuring time for write
			 * operation
			 */
			long initialTime = System.currentTimeMillis();
			/**
			 * outer for loop to iterate for N times to write
			 */
			for (int j = 1; j <= N; j++) {
				// System.out.println("Reading " + j + "th element");
				it = inputStreamList.iterator();
				/**
				 * Iterate the inputStreamList to get each inputStream and wrap
				 * it with DataInputStream and perform write
				 */
				while (it.hasNext()) {
					fileIn = it.next();
					dataInputSt = new DataInputStream(fileIn);
					dataInputSt.readInt();
					// System.out.println(dataInputSt.readInt());

				}
			}

			long timeToComplete = System.currentTimeMillis() - initialTime;
			System.out.println("Total time required to read the data file-->"
					+ timeToComplete + "ms");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			while (it.hasNext()) {

				try {
					dataInputSt.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

	}
}
