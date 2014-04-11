package ulb.cs.dsa.streams;

import static java.lang.Integer.MAX_VALUE;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MapLargeFile {

	public static final int BUFFER_SIZE = 1024 * 4; // 50MB
	public static final long FILELENGTH = BUFFER_SIZE * 20L * 1000L;// 1GB
	public static final byte[] EMPTY_PAGE = new byte[BUFFER_SIZE];
	long initialTime = 0L;
	public static long totalTime = 0L;
	/**
	 * N:-The input data for writing
	 */
	// final static int N = 1000000000;

	/**
	 * The location of input (to read)/output(to write) files
	 */
	final static String INPUT_FILE_PATH = "E://inputfiles_buff//";
	/**
	 * The range of random numbers to be generated
	 */
	final static int RANDOM_RANGE = 1000;

	
	MapLargeFile(String fileName) throws Exception {
		preallocateTestFile(INPUT_FILE_PATH + fileName);
		
				
		//this.totalTime += write(INPUT_FILE_PATH + fileName);
		MapLargeFile.totalTime += readFiles(INPUT_FILE_PATH+fileName);
		System.out.println("Time taken to read--"+MapLargeFile.totalTime);
	}

	private void preallocateTestFile(final String fileName) throws Exception {
		RandomAccessFile file = new RandomAccessFile(fileName, "rw");

		for (long i = 0; i < FILELENGTH; i += BUFFER_SIZE) {
			file.write(EMPTY_PAGE, 0, BUFFER_SIZE);
		}

		file.close();
	}

	public long write(final String fileName) throws Exception {
		System.gc();
		FileChannel channel = new RandomAccessFile(fileName, "rw").getChannel();
		MappedByteBuffer buffer = channel.map(READ_WRITE, 0,
				Math.min(channel.size(), MAX_VALUE)*4);
		
		initialTime = System.currentTimeMillis();
		for (long i = 0; i < FILELENGTH; i++) { // mapping the buffer with new
												// size if the the previous
												// Mapped Buffer is filled out
			if (!buffer.hasRemaining()) {
				buffer = channel.map(READ_WRITE, i,
						Math.min(channel.size() - i, MAX_VALUE*4));
			}
			buffer.putInt(77);
		}
		totalTime = System.currentTimeMillis() - initialTime;

		channel.close();

		return totalTime;
	}

	public long readFiles(final String fileName) throws Exception {
		System.gc();
		FileChannel fileChannel = new RandomAccessFile(fileName, "rw")
				.getChannel();
		IntBuffer buffer = fileChannel.map(READ_ONLY, 0,
				Math.min(fileChannel.size(), MAX_VALUE)*4).asIntBuffer();
		initialTime = System.currentTimeMillis();
		for (long i = 0; i < FILELENGTH; i++) {
			if (!buffer.hasRemaining()) {
				buffer = fileChannel.map(READ_ONLY, i,
						Math.min(fileChannel.size() - i, MAX_VALUE)*4)
						.asIntBuffer();
			}

			buffer.get((int) i);
		}
		totalTime = System.currentTimeMillis() - initialTime;
		fileChannel.close();

		return totalTime;
	}

}
