package hama.strassen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
	private static final String blockSizeString = "strassen.blocksize";
	private static final String inputMatrixARows = "strassen.nbRowA";
	private static final String inputMatrixACols = "strassen.nbColA";
	private static final String inputMatrixBRows = "strassen.nbRowB";
	private static final String inputMatrixBCols = "strassen.nbColB";

	public static class StrassenBSP
			extends
			BSP<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> {

		private int nbRowsA;
		private int nbColsA;
		private int nbRowsB;
		private int nbColsB;
		private int blockSize = 2;
		private Matrix a;
		private Matrix b;
		private static HashMap<Integer, List<Matrix>> aBlocks;
		private static HashMap<Integer, List<Matrix>> bBlocks;
		private static HashMap<Integer, List<Matrix>> resBlocks;
		private static HashMap<Integer, List<Integer[]>> indices;

		@Override
		public void setup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException, SyncException, InterruptedException {
			HamaConfiguration conf = peer.getConfiguration();
			blockSize = conf.getInt(blockSizeString, 2);
			if (peer.getPeerIndex() == 0) {
				aBlocks = new HashMap<Integer, List<Matrix>>();
				bBlocks = new HashMap<Integer, List<Matrix>>();

				/* Values for rows and columns padded */
				nbRowsA = Utils.getBlockMultiple(
						conf.getInt(inputMatrixARows, 4), blockSize);
				nbColsA = Utils.getBlockMultiple(
						conf.getInt(inputMatrixACols, 4), blockSize);
				nbRowsB = Utils.getBlockMultiple(
						conf.getInt(inputMatrixBRows, 4), blockSize);
				nbColsB = Utils.getBlockMultiple(
						conf.getInt(inputMatrixBCols, 4), blockSize);
				Matrix a = new Matrix(Utils.readMatrix(
						new Path(conf.get(inputMatrixAPathString)),
						peer.getConfiguration(),
						conf.getInt(inputMatrixARows, 4),
						conf.getInt(inputMatrixACols, 4), blockSize), nbRowsA,
						nbColsA);
				Matrix b = new Matrix(Utils.readMatrix(
						new Path(conf.get(inputMatrixBPathString)),
						peer.getConfiguration(),
						conf.getInt(inputMatrixBRows, 4),
						conf.getInt(inputMatrixBCols, 4), blockSize), nbRowsB,
						nbColsB);
				System.out.println("A");
				a.print();
				System.out.println("B");
				b.print();
				System.out.println("A*B");
				System.out.println(a.mult(b).toString());
				int peerInd = 0;
				indices = new HashMap<Integer, List<Integer[]>>();
				for (int i = 0; i < nbRowsA / blockSize; i++) {
					for (int j = 0; j < nbColsB / blockSize; j++) {
						for (int k = 0; k < nbColsA / blockSize; k++) {
							Matrix aBlock = a.getBlock(i, k, blockSize);
							Matrix bBlock = b.getBlock(k, j, blockSize);
							List<Matrix> peerABlocks = aBlocks.get(peerInd);
							if (peerABlocks==null) {
								peerABlocks = new ArrayList<Matrix>();
							}
							peerABlocks.add(aBlock);
							aBlocks.put(peerInd, peerABlocks);
							
							List<Matrix> peerBBlocks = bBlocks.get(peerInd);
							if (peerBBlocks==null) {
								peerBBlocks = new ArrayList<Matrix>();
							}
							peerBBlocks.add(bBlock);
							bBlocks.put(peerInd, peerBBlocks);

							List<Integer[]> peerIndices = indices.get(peerInd);
							if (peerIndices==null) {
								peerIndices = new ArrayList<Integer[]>();
							}
							peerIndices.add(new Integer[]{ i, j});
							indices.put(peerInd, peerIndices);
							peerInd = (peerInd+1)%peer.getNumPeers();
						}
					}
				}
			}
			peer.sync();
		}

		@Override
		public void bsp(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException, SyncException, InterruptedException {
			List<Matrix> peerABlocks = aBlocks.get(peer.getPeerIndex());
			List<Matrix> peerBBlocks = bBlocks.get(peer.getPeerIndex());
			for (int i=0; i<peerABlocks.size();i++){
				Matrix aBlock = peerABlocks.get(i);
				Matrix bBlock = peerBBlocks.get(i);
				Matrix resBlock = aBlock.strassen(bBlock);
				ResultMessage mes = new ResultMessage(peer.getPeerIndex(),
						resBlock);
				peer.send(peer.getPeerName(0), mes);
			}
			peer.sync();
		}

		@Override
		public void cleanup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException {
			if (peer.getPeerIndex() == 0) {
				resBlocks = new HashMap<Integer, List<Matrix>>();
				ResultMessage result = peer.getCurrentMessage();
				while (result != null) {
					int peerInd = result.getSender();
					Matrix block = result.getResult();
					List<Matrix> peerResBlocks = resBlocks.get(peerInd);
					if (peerResBlocks==null){
						peerResBlocks = new ArrayList<Matrix>();
					}
					peerResBlocks.add(block);
					resBlocks.put(peerInd, peerResBlocks);
					result = peer.getCurrentMessage();
				}

				Matrix[][] cBlocks = new Matrix[nbRowsA / blockSize][nbColsB
						/ blockSize];
				for (int i = 0; i < nbRowsA / blockSize; i++) {
					for (int j = 0; j < nbColsB / blockSize; j++) {
						cBlocks[i][j] = new Matrix(blockSize, blockSize);
						cBlocks[i][j].zeroes();
					}
				}
				for (Integer peerInd : indices.keySet()) {
					List<Matrix> peerResBlocks = resBlocks.get(peerInd);
					List<Integer[]> peerIndices = indices.get(peerInd);
					for (int i=0; i<peerResBlocks.size(); i++){
						Matrix resBlock = peerResBlocks.get(i);
						Integer[] inds = peerIndices.get(i);
						cBlocks[inds[0]][inds[1]] = cBlocks[inds[0]][inds[1]]
								.sum(resBlock);
					}
				}

				System.out.println("C");
				Matrix c = new Matrix(cBlocks, nbRowsA, nbColsB, blockSize);
				c.print();
			}
		}

	}

	public static void main(String[] args) throws Exception {
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf);
		// Set the job name
		bsp.setJobName("Strassen Multiply");
		bsp.setBspClass(StrassenBSP.class);
		//bsp.setJar("strassen.jar");

		// DELETE ////
		bsp.setOutputPath(new Path("src/out"));
		bsp.setNumBspTask(8);
		conf.set(inputMatrixAPathString, "src/a");
		conf.set(inputMatrixBPathString, "src/b");
		// /////////

		if (args.length < 6 || args.length > 9) {
			printUsage();
			// TODO return;
		} else {
			if (parseArgs(args, conf, bsp) != 0) {
				// TODO return;
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
		if (args.length > 7) {
			bsp.setNumBspTask(Integer.parseInt(args[7]));
			if (args.length > 8) {
				int n = Integer.parseInt(args[8]);
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

				int nbPeers = Integer.parseInt(args[7]);
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
						+ "<path B> <number rows B> <number columns B> <path output> [number of tasks] [block size]");
	}
}
