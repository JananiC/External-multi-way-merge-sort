/****************************************************************
 * Class: BufferedIOStream.java
 * Description: This class demonstrates the reading and writing 
 * 				operations using BufferedOutputStream and 
 				BufferedInputStream streams
 * Date: 11/05/2012
 * @author JANANI CHAKKARADHARI, STEPHANY GARCIA
 *
 ***************************************************************/

package ulb.cs.dsa.streams;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;

public class BufferedIOStream {

	/**
	 * N:-The input data for writing - Change this while executing based on the experiment
	 */
	final static int N = 1000000;
	/**
	 * K:-The number of I/O streams - Change this while executing based on the experiment
	 */
	final static int K = 100;
	/**
	 * The location of input (to read)/output(to write) files
	 */
	final static String INPUT_FILE_PATH = "E://MMB_TEST//Largefile//";
	/**
	 * The range of random numbers to be generated
	 */
	final static int RANDOM_RANGE = 1000;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedIOStream bufIO = new BufferedIOStream();
		bufIO.writeDataFiles();
		bufIO.readDataFiles();

	}

	/**
	 * . Method to write data on "K" files using BufferedOutputStream
	 */
	public void writeDataFiles() {

		DataOutputStream dos = null;
		BufferedOutputStream bufOut = null;
		Iterator<OutputStream> outListIter = null;
		Iterator<BufferedOutputStream> bufferedOutListIter = null;

		try {
			/**
			 * Creating an ArrayList of OutputStream for holding K
			 * FileOutputStreams of Files
			 */
			ArrayList<OutputStream> outputStreamList = new ArrayList<OutputStream>();
			/**
			 * Creating an ArrayList of bufferedOutList for holding K
			 * BufferedOutputStreams of Outputstreams
			 */
			ArrayList<BufferedOutputStream> bufferedOutList = new ArrayList<BufferedOutputStream>();

			int i = 1;
			/**
			 * 
			 */
			while (i <= K) {
				outputStreamList.add(new FileOutputStream(INPUT_FILE_PATH
						+ "File" + i));
				// System.out.println("Opened file in write--" + i);
				i++;
			}

			outListIter = outputStreamList.iterator();
			while (outListIter.hasNext()) {

				bufferedOutList
						.add(new BufferedOutputStream(outListIter.next()));

			}

			Common com = new Common(RANDOM_RANGE);// Random ran = new Random();
			/**
			 * . Initializing the current time for measuring time for write
			 * operation
			 */
			long initialTime = System.currentTimeMillis();
			/**
			 * outer for loop to iterate for N times to write
			 */
			for (int j = 1; j <= N; j++) {
				int randomNum = com.getRandomNum();
				bufferedOutListIter = bufferedOutList.iterator();
				/**
				 * Iterate the bufferedOutListIter to get each bufferedOutput
				 * Stream and wrap it with DataOutputStream and perform write
				 * into buffer
				 */
				while (bufferedOutListIter.hasNext()) {

					bufOut = bufferedOutListIter.next();
					dos = new DataOutputStream(bufOut);
					dos.writeInt(randomNum);

				}

			}

			bufferedOutListIter = bufferedOutList.iterator();
			/**
			 * Finally writing to the disk by one time flushing each buffered
			 * stream
			 */
			while (bufferedOutListIter.hasNext()) {
				bufferedOutListIter.next().flush();
			}
			long timeToComplete = System.currentTimeMillis() - initialTime;
			System.out.println("WRITE IN MILLI SECONDS=====>"
					+ timeToComplete + " ms");
			System.out.println("WRITE IN SECONDS=====>"
					+ new Common().getSeconds(timeToComplete) + " s");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			/**
			 * Closing all the files
			 */
			while (bufferedOutListIter.hasNext()) {
				try {
					dos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		}

	}

	/**
	 * . Method to read data on "K" files using BufferedInputStream
	 */
	public void readDataFiles() {

		Iterator<InputStream> it = null;
		InputStream fileIn = null;
		Iterator<DataInputStream> dataInIter = null;
		ArrayList<DataInputStream> dataInStreamList = new ArrayList<DataInputStream>();
		ArrayList<InputStream> inputStreamList = new ArrayList<InputStream>();
		ArrayList<BufferedInputStream> buffInStreamList = new ArrayList<BufferedInputStream>();
		long initialTime = 0L;
		long timeToComplete = 0L;

		try {

			File folder = new File(INPUT_FILE_PATH);
			File[] listOfFiles = folder.listFiles();
			int i = 1;
			/**
			 * Creating Arraylist of Input Stream list (inputStreamList) for K
			 * reading files
			 */
			while (i <= K) {
				inputStreamList.add(new FileInputStream(listOfFiles[i - 1]));
				// System.out.println("Opened file in read--" + i);
				i++;
			}

			it = inputStreamList.iterator();
			/**
			 * Wrapping K inputstream list with BufferedInputStream list
			 * (buffInStreamList)
			 */
			while (it.hasNext()) {
				fileIn = it.next();
				buffInStreamList.add(new BufferedInputStream(fileIn));

			}

			/**
			 * Wrapping K buffered Input streams with DataInput stream
			 * (dataInStreamList)
			 */
			Iterator<BufferedInputStream> it1 = buffInStreamList.iterator();
			while (it1.hasNext()) {
				BufferedInputStream bIn = it1.next();
				dataInStreamList.add(new DataInputStream(bIn));

			}
			initialTime = System.currentTimeMillis();
			/**
			 * Reading One element from each dataInputstream recursively till N
			 */
			for (int j = 1; j <= N; j++) {
				dataInIter = dataInStreamList.iterator();
				while (dataInIter.hasNext()) {
					dataInIter.next().readInt();
				}
			}

			timeToComplete = System.currentTimeMillis() - initialTime;
			System.out.println("READ IN MILLI SECONDS=====>"
					+ timeToComplete + " ms");
			System.out.println("READ IN SECONDS=====>"
					+ new Common().getSeconds(timeToComplete) + " s");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (EOFException ex) {
			ex.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			/**
			 * Closing the list of opened streams
			 */
			while (dataInIter.hasNext()) {
				try {
					dataInIter.next().close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		}

	}

}
