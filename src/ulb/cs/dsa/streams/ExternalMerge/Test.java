package ulb.cs.dsa.streams.ExternalMerge;

import java.util.List;

import ulb.cs.dsa.streams.Common;
/**
 * 
 * @author JANANI CHAKKARADHARI,STEPHANY GARCIA
 * 
 * This class is for IO testing Memory mapping
 *
 */
public class Test implements CommonInter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		MemoryMapOut memOut = new MemoryMapOut();
		List<OutputStreamInter> list = memOut.writeFiles(INPUT_LARGE_FILE, K);
		Common com = new Common(RANDOM_RANGE);
		long init = System.currentTimeMillis();
		for (int j = 0; j <= N; j++) {
			for (OutputStreamInter ls : list) {
				int randomNum = com.getRandomNum();
				if (j == N) {
					//System.out.println(-1);
					ls.writeNext(-1);
				} else {
					ls.writeNext(randomNum);
					//System.out.println(randomNum);

				}
			}
		}
		memOut.closeOutputStream();
		System.out.println("Time for write--"+(System.currentTimeMillis()- init)+"ms");
	
		// TODO Auto-generated method stub
		
		MemoryMapIn mm = new MemoryMapIn();
		//InputStreamInter inputSt = mm.getInputStream(inputFile.getAbsolutePath());
		List<InputStreamInter> list1 = mm.readFiles(INPUT_LARGE_FILE);
		long init1 = System.currentTimeMillis();
		for (int x = 0; x <= N; x++) {
			for(InputStreamInter inputSt : list1){
				inputSt.next();
			}
			
			//System.out.println("======================" + x);
		}
		System.out.println("Time for read--"+(System.currentTimeMillis()- init1)+"ms");
		mm.closeInputStream();


	}

}
