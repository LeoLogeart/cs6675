package hama.strassen.block;

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
			BSP<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> {

		private int nbRowsA;
		private int nbColsB;
		private int blockSize = 2;

		@Override
		public void setup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
				throws IOException, SyncException, InterruptedException {
			HamaConfiguration conf = peer.getConfiguration();
			blockSize = conf.getInt(blockSizeString, 2);

			/* Values for rows and columns padded */
			nbRowsA = Utils.getBlockMultiple(conf.getInt(inputMatrixARows, 4),
					blockSize);
			nbColsB = Utils.getBlockMultiple(conf.getInt(inputMatrixBCols, 4),
					blockSize);
		}

		@Override
		public void bsp(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
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

			Utils.readMatrixBlocks(conf.get(inputMatrixAPathString), peer.getConfiguration(), rows, cols, blockSize, aILast, aBlocks);
			rows = conf.getInt(inputMatrixBRows, 4);
			cols = conf.getInt(inputMatrixBCols, 4);
			Utils.readMatrixBlocks(conf.get(inputMatrixBPathString), peer.getConfiguration(), rows, cols, blockSize, bILast, bBlocks);
			
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
				resBlock.setBlockIndex(aBlock.getI(), bBlock.getJ());
				resBlock = resBlock.sum(aBlock.getBlock().strassen(bBlock.getBlock()));
				resBlocks.put(ind, resBlock);
			}
			
			for (String ind : resBlocks.keySet()){
				Matrix block = resBlocks.get(ind);
				Path path = new Path(conf.get(inputMatrixCPathString)+block.getIndI()+"_"+block.getIndJ()+".mat");
				Utils.writeMatrix(block.getValues(), path, conf);
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

		if (args.length < 8 || args.length > 9) {
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
		conf.set(inputMatrixCPathString, args[6]);
		conf.setInt(blockSizeString, Integer.parseInt(args[7]));
		if (args.length > 8) {
			bsp.setNumBspTask(Integer.parseInt(args[8]));
		}  else {
			bsp.setNumBspTask(Integer.parseInt(args[1])/Integer.parseInt(args[7]));
		}
		
		return 0;
	}

	private static void printUsage() {
		System.out
				.println("Usage: StrassenMultiply <path A> <number rows A> <number columns A>"
						+ "<path B> <number rows B> <number columns B> <path output> <block size> [number of tasks]");
	}
}
