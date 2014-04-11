package ulb.cs.dsa.streams.ExternalMerge;
/**
 * 
 * @author JANANI CHAKKARADHARI, STEPHANY GARCIA
 *
 */
public interface CommonInter {
	/**Need to vary for testing**/
	/*******************************************************/
	static int MEMORY_MAP_SIZE = 64;
	final static int N = 1000;
	static double M = 512;
	static double dway = 3;
	/*******************************************************/
	long fileSize = N;
	
	/**
	 * K:-The number of I/O streams. Here K=1 for testing external sorting.
	 * Because this K is the number of files.So we will be creating 1
	 *  big file by running MemeoryMapOut.java. 
	 *  
	 *  For IO testing K can be varied
	 */
	final static int K = 1;
	/**
	 * The range of random numbers to be generated
	 */
	final static int RANDOM_RANGE = 10000;
	/**
	 * The location of large input (to read)/ 
	 */
	final static String INPUT_LARGE_FILE = "D://MMB_TEST//Largefile//";
	/**
	 * The location of split files and merged files
	 */
	final static String SPLIT_FILE_PATH = "D://MMB_TEST//file_cunks//";

}
