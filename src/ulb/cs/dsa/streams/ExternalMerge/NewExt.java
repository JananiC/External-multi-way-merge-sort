package ulb.cs.dsa.streams.ExternalMerge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * 
 * @author JANANI CHAKKARADHARI,STEPHANY GARCIA
 *
 */
public class NewExt implements CommonInter {

	/**
	 * @param args
	 */
	
	static double d = 0;
	static double noOfIntegers = 0;
	static long mergeSortTime = 0;
	long sortTime = 0;
	long init = 0;
	long finiTime = 0;
	static int mergepasscount = 0;
	
	static MemoryMapIn memIn = new MemoryMapIn();
	static MemoryMapOut memOut = new MemoryMapOut();
	
	
	
	static InputStreamInter lastStream = null;
	
	boolean flag = false;
	

	public static void main(String[] args) throws IOException {
		
		NewExt newExt = new NewExt();
		newExt.splitFile(INPUT_LARGE_FILE + "File1");
		//rough estimate of noOf passes in the beginning
		double noOfPasses =  Math.ceil(d / dway);
		
		//System.out.println(d+">>>"+dway);
		for (int i = 0; i < noOfPasses;) {
			String newlocation = SPLIT_FILE_PATH + "Merge" + i;
			int retVal = newExt.multiWayMerge(newlocation, ++i, dway);
			if(retVal == 1){
				//System.out.println("break");
				break;
			}
		}
		memIn.closeInputStream();
		memOut.closeOutputStream();
		System.out.println("Total no of passes in merging--"+(mergepasscount));
		System.out.println("Time taken to merge chunks of files--" + mergeSortTime);
	}

	/**
	 * This method does the following 1. Divides(splitFile-method) the given
	 * input files (from loc:INPUT_LARGE_FILE) into no. of chunks 2. Sorts each
	 * chunk using Arrays.sort and writes back to disk in the
	 * loc:SPLIT_FILE_PATH 3. calls the "merge" method (of section 1.2) which
	 * writes the merged list into the disk
	 * 
	 * @throws IOException
	 */
	int multiWayMerge(String filepathmerge, int passNum, double dWay)
			throws IOException {
		mergepasscount++;
		int sizelist = 0;
		//Reading the inputfiles and storing the references in the list
		List<InputStreamInter> list = memIn.readFiles(filepathmerge);
		if (list.size() == 1) {
			mergepasscount = mergepasscount-1;
			System.out.println("Total no of passes in merging--"+mergepasscount);
			System.out.println("Time taken to merge chunks of files--" + mergeSortTime);
			return 1;
		}
		PriorityQMerge pq = new PriorityQMerge();
		//for even no. of files
		if(list.size()<dway){
			sizelist = list.size();
			//System.out.println("Janani==>"+list.size());
			iterateFiles(list, sizelist, list.size(), passNum, pq);

		}
		else if (list.size() % dway == 0) {
			sizelist = list.size();
			iterateFiles(list, sizelist, dWay, passNum, pq);

		}//for odd no. of files 
		else {
			sizelist = list.size()-1;
			lastStream = list.get(list.size() - 1);
			flag = true;
			iterateFiles(list, sizelist, dWay, passNum, pq);
		
		}
		memIn.closeInputStream();
		return 0;
	}
	
	void iterateFiles(List<InputStreamInter> list, int sizelist, double dWay, int passNum, PriorityQMerge pq){
		int filenameCnt = 1;
		OutputStreamInter outInStream = null;
		File dir = new File(SPLIT_FILE_PATH + "/Merge" + passNum);
		dir.mkdir();
		for (int h = 0; h < sizelist;) {
			// System.out.println("PPP"+h);
			ArrayList<InputStreamInter> interList = new ArrayList<InputStreamInter>(
					(int)dWay);
			for (int x = 0; x < dWay && h < sizelist; x++) {
				// System.out.println(h);
				interList.add(list.get(h));
				h++;
			}
			int len = 0;
			for(InputStreamInter iter : interList){
				len += iter.getNumOfIntegers();
			}
			//System.out.println("no of Integers --"+len);
			MemoryMapOut.lenofFile = len*4;
			outInStream = memOut.getOutputStream(dir
					.getAbsolutePath() + "//File" + filenameCnt);
			
			init = System.currentTimeMillis();			
			pq.merge(interList, outInStream);			
			finiTime = System.currentTimeMillis();
			mergeSortTime += (finiTime-init) ;
			filenameCnt++;
			
		}
		//only for odd number of files
		if(flag){
			flag = false;
			
			//System.out.println("last no of Integers --"+lastStream.getNumOfIntegers());
			MemoryMapOut.lenofFile = lastStream.getNumOfIntegers()*4;
			outInStream = memOut.getOutputStream(dir.getAbsolutePath()
					+ "//File" + filenameCnt);
			for (int i = 0; i < lastStream.getNumOfIntegers(); i++) {
				outInStream.writeNext(lastStream.next());
			}
		}
		//System.out.println("Total no passes in merge(Phase2)---->"+filenameCnt);
	}

	void splitFile(String filepath) throws IOException {

		File inputFile = new File(filepath);
		long lenOfFile = inputFile.length();
		System.out.println("len of file---->" + inputFile.length());
		noOfIntegers = M / 4; // total number of integers that can be stored in
		// Main memory -M
		d = (int) Math.ceil(lenOfFile / M); // total number of runs
		
		InputStreamInter inputSt = memIn.getInputStream(inputFile
				.getAbsolutePath());
		System.out.println("N--------" + lenOfFile);
		System.out.println("M--------" + M);
		System.out.println("dway-----" + dway);
		System.out.println("Total file chunks(Phase1)-"+d);
		System.out.println("Memory mapping buffer--" + MEMORY_MAP_SIZE);

		int j = 1;
		
		while (j <= d) {
			int toRead = 0;
			int rem = ((int) (lenOfFile % M)/4);
			
			if(rem!=0 && j==d){
				toRead = rem ;
			}else{
				
				toRead = (int) (M/4);
				
			}
			//System.out.println("===================="+toRead);
			List<Integer> buffer = readFile(inputSt,toRead);
			int[] sort = convertIntegers(buffer);
			long init = System.currentTimeMillis();
			Arrays.sort(sort);
			long finiTime = System.currentTimeMillis();
			sortTime = (finiTime - init);
			File dir = new File(SPLIT_FILE_PATH + "/Merge0");
			dir.mkdir();
			MemoryMapOut.lenofFile = sort.length*4;
			OutputStreamInter outst = memOut.getOutputStream(dir
					.getAbsolutePath() + "//File" + j);
			writeFile(outst, sort);
			// System.out.println(sortTime);
			j++;
		}

		
		
		System.out.println("Sorting time chuncks of files --" + sortTime);
		memIn.closeInputStream();
		memOut.closeOutputStream();

	}

	public static int[] convertIntegers(List<Integer> integers) {
		int[] ret = new int[integers.size()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = integers.get(i).intValue();
		}
		return ret;
	}

	List<Integer> readFile(InputStreamInter inputSt, int noOfIntToread) {
		ArrayList<Integer> buffer = new ArrayList<Integer>();
		for (int i = 1; i <= noOfIntToread; i++) {
			int val = inputSt.next();
			//System.out.println(val);
			if (val != -1) {
				buffer.add(val);
			} 
		}
		//System.out.println("==================================");
		return buffer;

	}

	void writeFile(OutputStreamInter outSt, int[] buffer) {
		//System.out.println("%%%%%%%%%%%%%%%-"+buffer.length);
		
		for (int j = 0; j < buffer.length; j++) {
			//System.out.println(buffer[j]);
			outSt.writeNext(buffer[j]);
		}
		
		//System.out.println("=====================================");
		memOut.closeOutputStream();
	}

}
