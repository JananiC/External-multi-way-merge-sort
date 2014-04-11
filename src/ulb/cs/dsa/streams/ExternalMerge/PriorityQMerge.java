package ulb.cs.dsa.streams.ExternalMerge;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * 
 * @author JANANI CHAKKARADHARI,STEPHANY GARCIA
 * 
 */
public class PriorityQMerge {
	
	int NOFINT = (int) Math.floor(NewExt.M/4);
	static int count = 0;
	/**
	 * This method writes the sorted merge list into the disk
	 * 
	 * @param sortedLists
	 * @param outPutStream
	 *            -
	 * 
	 *            Description: OutputStreamInter is a reference to the
	 *            underlying MemoryMapping Output stream Implementation. So
	 *            whenever the Memory mapped buffer is written with an
	 *            integer{put(write)} and it reaches its size, it will be written
	 *            to the disk and reading next block of M size is managed by the
	 *            underlying MemoryMapping class(MemoryMapOut.java)
	 */
	public void merge(List<InputStreamInter> sortedLists,
			OutputStreamInter outPutStream) {
		Queue<Entry> queue = new PriorityQueue<Entry>(CommonInter.MEMORY_MAP_SIZE);
		for (InputStreamInter ls : sortedLists) {	
			InputStreamInter it = ls;
				int val = ls.next();
				//System.out.println(val);
				queue.add(new Entry(val, it));
		}
		while (!queue.isEmpty()) {
			Entry entry = queue.poll();
			int value = entry.getValue();
			outPutStream.writeNext(value);
			if (entry.readNext())
				queue.add(entry);
		}
		outPutStream.closeOutputStream();
	}
	
} 

class Entry implements Comparable<Entry> {
	private int value;
	private InputStreamInter input;

	public Entry(int value, InputStreamInter input) {
		this.value = value;
		this.input = input;
	}

	public int getValue() {
		return this.value;
	}

	public boolean readNext() {
		value = input.next();
		if(value!=-1)	{		
		//System.out.println("Next value--"+value);
		return true;
		}
		else{
		return false;
		}
	}
	public int compareTo(Entry entry) {
		//System.out.println(this.value+"-"+entry.value);
		return this.value - entry.value;
	}

}
