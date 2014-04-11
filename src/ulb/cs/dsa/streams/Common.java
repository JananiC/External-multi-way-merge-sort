package ulb.cs.dsa.streams;

import java.util.Random;

public class Common {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
	}
	int range = 0;
	Random random = null;
	public Common(int range) {
		this.range = range;
		random = new Random();
		
	}
	public Common(){
		
	}
	
	public int getRandomNum() {
		int randNum = 0;
		randNum = random.nextInt(this.range);
		return randNum;
	}

public long getSeconds(long milli){
	return milli/1000;
}
}
