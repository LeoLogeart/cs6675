package hama.strassen.nosync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

public class StrassenMultiply {
	private static final String inputMatrixAPathString = "strassen.patha";
	private static final String inputMatrixBPathString = "strassen.pathb";
	private static final String inputMatrixCPathString = "strassen.pathc";
	private static final String blockSizeString = "strassen.blocksize";
	private static final String inputMatrixARows = "strassen.nbRowA";
	private static final String inputMatrixACols = "strassen.nbColA";
	private static final String inputMatrixBRows = "strassen.nbRowB";
	private static final String inputMatrixBCols = "strassen.nbColB";

	public static class StrassenBSP
			extends
			BSP<NullWritable, NullWritable, NullWritable, NullWritable, JobMessage> {

		private int nbRowsA;
		private int nbColsB;
		private int blockSize = 2;

		@Override
		public void setup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, JobMessage> peer)
				throws IOException, SyncException, InterruptedException {
			HamaConfiguration conf = peer.getConfiguration();
			blockSize = conf.getInt(blockSizeString, 2);

			/* Values for rows and columns padded */
			nbRowsA = Utils.getBlockMultiple(conf.getInt(inputMatrixARows, 4),
					blockSize);
//			nbColsA = Utils.getBlockMultiple(conf.getInt(inputMatrixACols, 4),
//					blockSize);
//			nbRowsB = Utils.getBlockMultiple(conf.getInt(inputMatrixBRows, 4),
//					blockSize);
			nbColsB = Utils.getBlockMultiple(conf.getInt(inputMatrixBCols, 4),
					blockSize);
		}

		@Override
		public void bsp(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, JobMessage> peer)
				throws IOException, SyncException, InterruptedException {
			int nbRowsBlocks = nbRowsA / blockSize;
			int nbColsBlocks = nbColsB / blockSize;
			int nbBlocks = nbRowsBlocks * nbColsBlocks;
			int blocksPerPeer = nbBlocks / peer.getNumPeers();
			int p = peer.getPeerIndex();
			int lastBlock = (p+1)*blocksPerPeer;
			if (p==peer.getNumPeers()-1) {
				lastBlock = nbBlocks;
			}
			ArrayList<Block> aBlocks = new ArrayList<Block>();
			ArrayList<Block> bBlocks = new ArrayList<Block>();
			for (int block = p*blocksPerPeer; block<lastBlock; block++){
				int i = block/nbColsBlocks;
				int j = block%nbColsBlocks;			
				for (int k = 0; k < nbRowsBlocks; k++) {
					Block aBlock = new Block(i, k, blockSize);
					aBlocks.add(aBlock);
					Block bBlock = new Block(k,j,blockSize);
					bBlocks.add(bBlock);
				}
			}
			
			int aILast = ((lastBlock-1)/nbColsBlocks)*blockSize+blockSize;
			int bILast = nbRowsBlocks*blockSize;
			
			HamaConfiguration conf = peer.getConfiguration();
			int rows = conf.getInt(inputMatrixARows, 4);
			int cols = conf.getInt(inputMatrixACols, 4);
			Utils.readMatrixBlocks(new Path(conf.get(inputMatrixAPathString)), peer.getConfiguration(), rows, cols, blockSize, aILast, aBlocks);
			rows = conf.getInt(inputMatrixBRows, 4);
			cols = conf.getInt(inputMatrixBCols, 4);
			Utils.readMatrixBlocks(new Path(conf.get(inputMatrixBPathString)), peer.getConfiguration(), rows, cols, blockSize, bILast, bBlocks);


			HashMap<String, Matrix> resBlocks = new HashMap<>();
			
			for (int b = 0; b<aBlocks.size(); b++){
				Block aBlock = aBlocks.get(b);
				Block bBlock = bBlocks.get(b);
				String ind = aBlock.getI() + "," + bBlock.getJ();
				Matrix resBlock = resBlocks.get(ind);
				if (resBlock==null){
					resBlock = new Matrix(blockSize,blockSize);
					resBlock.zeroes();
				}
				resBlock = resBlock.sum(aBlock.getBlock().strassen(bBlock.getBlock()));
				resBlocks.put(ind, resBlock);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf);
		// Set the job name
		bsp.setJobName("Strassen Multiply");
		bsp.setBspClass(StrassenBSP.class);
		bsp.setJar("strassen.jar");

		if (args.length < 6 || args.length > 10) {
			printUsage();
			return;
		} else {
			if (parseArgs(args, conf, bsp) != 0) {
				return;
			}
		}

		long startTime = System.currentTimeMillis();
		if (bsp.waitForCompletion(true)) {
			System.out.println("Job Finished in "
					+ (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds.");
		}
		/*
		double[][] cValues = Utils.readMatrix(new Path(args[7]), conf,
				Integer.parseInt(args[1]), Integer.parseInt(args[5]),
				Integer.parseInt(args[9]));
		Matrix c = new Matrix(cValues, Integer.parseInt(args[1]),
				Integer.parseInt(args[5]));
		c.print();*/

	}

	private static int parseArgs(String[] args, HamaConfiguration conf,
			BSPJob bsp) {
		conf.set(inputMatrixAPathString, args[0]);
		conf.setInt(inputMatrixARows, Integer.parseInt(args[1]));
		conf.setInt(inputMatrixACols, Integer.parseInt(args[2]));
		conf.set(inputMatrixBPathString, args[3]);
		conf.setInt(inputMatrixBRows, Integer.parseInt(args[4]));
		if (Integer.parseInt(args[2]) != Integer.parseInt(args[4])) {
			System.out.println("Matrices size do not match.");
			return 1;
		}
		conf.setInt(inputMatrixBCols, Integer.parseInt(args[5]));
		bsp.setOutputPath(new Path(args[6]));
		conf.set(inputMatrixCPathString, args[7]);
		if (args.length > 8) {
			bsp.setNumBspTask(Integer.parseInt(args[8]));
			if (args.length > 9) {
				int n = Integer.parseInt(args[9]);
				if ((n & (n - 1)) != 0) {
					System.out.println("The block size must be a power of two");
					return 1;
				}
				conf.setInt(blockSizeString, n);
			} else {
				/*
				 * set the size of blocks depending on the number of peers and
				 * matrices sizes
				 */
				/*
				 * let's assume square matrices : nbTasks =
				 * (PaddedRowSize/sizeBlock)^3 ,
				 */
				int rows = conf.getInt(inputMatrixARows, 4);
				int n = 2;
				/* set n as the largest power of two smaller than row/2 */
				while (n < rows / 2) {
					n *= 2;
				}
				n /= 2;

				int nbPeers = Integer.parseInt(args[8]);
				/*
				 * lower the block size to make sure every peer has at least a
				 * task
				 */
				while (Math.pow(rows / n, 3) < nbPeers) {
					n /= 2;
				}
				/* taking the future padding into account */
				n *= 2;
				System.out.println("block size :" + n);
				conf.setInt(blockSizeString, n);
			}
		} else {
			/*
			 * set the size of blocks depending on the number of peers and
			 * matrices sizes
			 */
			/*
			 * let's assume square matrices : nbTasks =
			 * (PaddedRowSize/sizeBlock)^3 ,
			 */
			int rows = conf.getInt(inputMatrixARows, 4);
			int n = 2;
			/* set n as the largest power of two smaller than row/4 */
			while (n < rows / 4) {
				n *= 2;
			}
			conf.setInt(blockSizeString, n);
			int tasks = (int) Math.pow((int) (rows / n), 3);
			System.out.println("block size :" + n);
			System.out.println("peers :" + tasks);
			bsp.setNumBspTask(tasks);
		}
		return 0;
	}

	private static void printUsage() {
		System.out
				.println("Usage: StrassenMultiply <path A> <number rows A> <number columns A>"
						+ "<path B> <number rows B> <number columns B> <path output> <path C> [number of tasks] [block size]");
	}
}
