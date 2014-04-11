package ulb.cs.dsa.streams.ExternalMerge;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
/**
 * 
 * @author JANANI CHAKKARADHARI,STEPHANY GARCIA
 *
 */
public class MemoryMapIn implements InputStreamInter {
	RandomAccessFile randomAccess;
	IntBuffer buffer;
	FileChannel fileCh;
	boolean flag=false;
	long pos = 0;
	long count = 1;

	MemoryMapIn() {
	}

	MemoryMapIn(FileChannel file) {
		setFileCh(file);
		buffer = allocate();
	}

	public FileChannel getFileCh() {
		return fileCh;
	}

	public void setFileCh(FileChannel fileCh) {
		this.fileCh = fileCh;
	}

	public IntBuffer allocate() {
		IntBuffer buffer = null;
		long diffCheck = 0 ;
		try {	
			diffCheck = getFileCh().size()-pos;
			if(diffCheck <= 0){
				//System.out.println("=="+getFileCh().size()+" pos=="+pos);
				flag=true;
			}else if
			(diffCheck < MEMORY_MAP_SIZE ){
				buffer = getFileCh().map(READ_ONLY, pos, diffCheck)
						.asIntBuffer();
			}else if(pos< getFileCh().size()){
				buffer = getFileCh().map(READ_ONLY, pos, MEMORY_MAP_SIZE)
						.asIntBuffer();
			} else{
				flag=true;
			}
			
		} catch (IOException e) {
			
			e.printStackTrace();
			
		}
		return buffer;
	}

	public int next() {
		if (!buffer.hasRemaining()) {
			return readFromNextBloc();
		} else {
			return  buffer.get();
		}
	}

	public InputStreamInter getInputStream(String filename) {
		InputStreamInter inputStr;
		setRandomAccessFile(filename);
		setFileChannel(randomAccess);
		pos = 0;
		inputStr = new MemoryMapIn(fileCh);
		return inputStr;
	}

	public List<InputStreamInter> readFiles(String filePath) {
		List<InputStreamInter> list = new ArrayList<InputStreamInter>();
		File folder = new File(filePath);
		File[] listOfFiles;
		listOfFiles = folder.listFiles();

		int i = 1;
		while (i <= listOfFiles.length) {
			list.add(getInputStream(listOfFiles[i - 1].getAbsolutePath()));
			i++;
		}
		return list;
	}

	public void setFileChannel(RandomAccessFile randomAcc) {
		fileCh = randomAcc.getChannel();
	}

	public void setRandomAccessFile(String filePath) {
		try {
			randomAccess = new RandomAccessFile(filePath, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	public int readFromNextBloc() {
		pos += MEMORY_MAP_SIZE;
		buffer = allocate();
		if(flag){
			return -1;
		}else{
		return  buffer.get();
		}
	}



	public void closeInputStream() {
		try {
			
			if (fileCh != null){
			//System.out.println("Closing the filechannel");
				fileCh.close();
			}
			if(randomAccess!=null){
				//System.out.println("closting file");
			randomAccess.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int getNumOfIntegers() {
		int size = 0;
		try {
			size = (int) getFileCh().size() ;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return size/4;
	}

	@Override
	public long getPosition() {
		return pos;
	}
	
	/*public static void main(String[] arg) {
	MemoryMapIn mm = new MemoryMapIn();
	// List<InputStreamInter> list = mm.readFiles(INPUT_LARGE_FILE);
	List<InputStreamInter> list = mm.readFiles(SPLIT_FILE_PATH+"Merge6//");
	long init = System.currentTimeMillis();
	long inputFileLen = 0 ;
	try {
		inputFileLen = mm.getFileCh().size();
		System.out.println("File length--"+inputFileLen);
	} catch (IOException e) {
		e.printStackTrace();
	}
	for (int x = 0; x < (inputFileLen/4); x++) {
		for (InputStreamInter inputSt : list) {
		//	for (int x = 0; x < (inputSt.getNumOfIntegers()); x++) {
			// inputSt.next();
			System.out.println(inputSt.next());
			}
		//}
		
	}
	
	System.out.println("Time for read--"
			+ (System.currentTimeMillis() - init) + "ms");
	mm.closeInputStream();

	}*/
	
}
