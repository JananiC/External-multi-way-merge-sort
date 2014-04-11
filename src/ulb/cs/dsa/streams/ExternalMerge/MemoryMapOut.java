package ulb.cs.dsa.streams.ExternalMerge;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import ulb.cs.dsa.streams.Common;
/**
 * 
 * @author JANANI CHAKKARADHARI,STEPHANY GARCIA
 *
 */
public class MemoryMapOut implements OutputStreamInter {
	RandomAccessFile randomAccess;
	IntBuffer buffer;
	FileChannel fileCh;
	boolean flag = false;
	static long lenofFile = (fileSize*4);
	int count = 1;
	long pos = 0;

	public FileChannel getFileCh() {
		return fileCh;
	}


	public List<OutputStreamInter> writeFiles(String filePath, int noOfStreams) {
		List<OutputStreamInter> outList = new ArrayList<OutputStreamInter>();
		int i = 1;
		while (i <= noOfStreams) {
			outList.add(getOutputStream(filePath + "File" + i));
			i++;
		}
		return outList;
	}

	public OutputStreamInter getOutputStream(String filename) {
		OutputStreamInter outPutStr;

		 setRandomAccessFile(filename);
		setFileChannel(randomAccess);
		pos = 0;
		outPutStr = new MemoryMapOut(fileCh);

		return outPutStr;
	}

	public void setFileChannel(RandomAccessFile randomAcc) {
		fileCh = randomAcc.getChannel();
	}

	public void setRandomAccessFile(String filePath) {
		try {
			randomAccess = new RandomAccessFile(filePath, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public IntBuffer allocate() {
		try {
			
			long lastBlock =  (lenofFile-pos);
			//System.out.println("=="+lastBlock);
			if(lastBlock <= 0){
				flag = true;
			}
			else if(lastBlock<MEMORY_MAP_SIZE){
				buffer = getFileCh().map(READ_WRITE, pos, lastBlock)
						.asIntBuffer();
			}else if(pos<lenofFile){
				buffer = getFileCh().map(READ_WRITE, pos, MEMORY_MAP_SIZE)
						.asIntBuffer();
			}else{
				flag = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer;
	}

	public void setFileCh(FileChannel fileCh) {
		this.fileCh = fileCh;
	}

	MemoryMapOut() {
	}

	MemoryMapOut(FileChannel file) {
		setFileCh(file);
		buffer = allocate();
	}
	public void writeToBloc(int val) {
			pos += MEMORY_MAP_SIZE;
			buffer = allocate();
			if(flag){
			}else{
			buffer.put(val);
			}
	}


	@Override
	public void writeNext(int val) {
		if (!buffer.hasRemaining()) {
			writeToBloc(val);
		} else {
			
			buffer.put(val);
		}
	}

	public void closeOutputStream() {
		try {
			if (fileCh != null){
			//System.out.println("closing in file channel");
				fileCh.close();
			} if(randomAccess!=null){
				//System.out.println("closing in file");
				randomAccess.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String arg[]) {
		MemoryMapOut memOut = new MemoryMapOut();
		List<OutputStreamInter> list = memOut.writeFiles(INPUT_LARGE_FILE, K);
		Common com = new Common(RANDOM_RANGE);
		long init = System.currentTimeMillis();
		for (int j = 0; j < N; j++) {
			for (OutputStreamInter ls : list) {
				int randomNum = com.getRandomNum();

					ls.writeNext(randomNum);
			}
		}
		System.out.println("Time for write--"
				+ (System.currentTimeMillis() - init) + "ms");
		memOut.closeOutputStream();
	}
}
