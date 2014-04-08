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
		private int nbColsA;
		private int nbRowsB;
		private int nbColsB;
		private int blockSize = 2;
		private Matrix a;
		private Matrix b;
		private HashMap<Integer, List<Matrix>> resBlocks;
		private HashMap<Integer, List<Integer[]>> indices;
		private List<Integer[]> masterIndices;

		@Override
		public void setup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, JobMessage> peer)
				throws IOException, SyncException, InterruptedException {
			HamaConfiguration conf = peer.getConfiguration();
			blockSize = conf.getInt(blockSizeString, 2);

			/* Values for rows and columns padded */
			nbRowsA = Utils.getBlockMultiple(conf.getInt(inputMatrixARows, 4),
					blockSize);
			nbColsA = Utils.getBlockMultiple(conf.getInt(inputMatrixACols, 4),
					blockSize);
			nbRowsB = Utils.getBlockMultiple(conf.getInt(inputMatrixBRows, 4),
					blockSize);
			nbColsB = Utils.getBlockMultiple(conf.getInt(inputMatrixBCols, 4),
					blockSize);
			a = new Matrix(Utils.readMatrix(
					new Path(conf.get(inputMatrixAPathString)),
					peer.getConfiguration(), conf.getInt(inputMatrixARows, 4),
					conf.getInt(inputMatrixACols, 4), blockSize), nbRowsA,
					nbColsA);
			b = new Matrix(Utils.readMatrix(
					new Path(conf.get(inputMatrixBPathString)),
					peer.getConfiguration(), conf.getInt(inputMatrixBRows, 4),
					conf.getInt(inputMatrixBCols, 4), blockSize), nbRowsB,
					nbColsB);
			if (peer.getPeerIndex() == 0) {
				/*
				 * System.out.println("A"); a.print(); System.out.println("B");
				 * b.print(); System.out.println("A*B");
				 * System.out.println(a.mult(b).toString());
				 */
			int peerInd = 0;
				indices = new HashMap<Integer, List<Integer[]>>();
				for (int i = 0; i < nbRowsA / blockSize; i++) {
					for (int j = 0; j < nbColsB / blockSize; j++) {
						for (int k = 0; k < nbColsA / blockSize; k++) {
							if (peerInd == 0) {
								if (masterIndices == null) {
									masterIndices = new ArrayList<Integer[]>();
								}
								masterIndices.add(new Integer[] { i, j, k });
							} else {
								JobMessage job = new JobMessage(0, i, j, k);
								peer.send(peer.getPeerName(peerInd), job);
							}

							List<Integer[]> peerIndices = indices.get(peerInd);
							if (peerIndices == null) {
								peerIndices = new ArrayList<Integer[]>();
							}
							peerIndices.add(new Integer[] { i, j });
							indices.put(peerInd, peerIndices);
							peerInd = (peerInd + 1) % peer.getNumPeers();
						}
					}
				}
			}
			peer.sync();
		}

		@Override
		public void bsp(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, JobMessage> peer)
				throws IOException, SyncException, InterruptedException {
			if (peer.getPeerIndex() == 0) {
				resBlocks = new HashMap<Integer, List<Matrix>>();
				resBlocks.put(0, new ArrayList<Matrix>());
				for (Integer[] inds : masterIndices) {
					Matrix aBlock = a.getBlock(inds[0], inds[2], blockSize);
					Matrix bBlock = b.getBlock(inds[2], inds[1], blockSize);
					Matrix resBlock = aBlock.strassen(bBlock);
					List<Matrix> peerBlocks = resBlocks.get(0);
					peerBlocks.add(resBlock);
				}
			} else {
				JobMessage result = peer.getCurrentMessage();
				while (result != null) {
					int i = result.getI();
					int j = result.getJ();
					int k = result.getK();
					Matrix aBlock = a.getBlock(i, k, blockSize);
					Matrix bBlock = b.getBlock(k, j, blockSize);
					Matrix resBlock = aBlock.strassen(bBlock);
					JobMessage mes = new JobMessage(1, peer.getPeerIndex(),
							resBlock);
					peer.send(peer.getPeerName(0), mes);
					result = peer.getCurrentMessage();
				}
			}
			peer.sync();
		}

		@Override
		public void cleanup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, JobMessage> peer)
				throws IOException {
			if (peer.getPeerIndex() == 0) {
				JobMessage result = peer.getCurrentMessage();
				while (result != null) {
					int peerInd = result.getSender();
					Matrix block = result.getResult();
					List<Matrix> peerResBlocks = resBlocks.get(peerInd);
					if (peerResBlocks == null) {
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
					for (int i = 0; i < peerResBlocks.size(); i++) {
						Matrix resBlock = peerResBlocks.get(i);
						Integer[] inds = peerIndices.get(i);
						cBlocks[inds[0]][inds[1]] = cBlocks[inds[0]][inds[1]]
								.sum(resBlock);
					}
				}

				Matrix c = new Matrix(cBlocks, nbRowsA, nbColsB, blockSize);
				HamaConfiguration conf = peer.getConfiguration();
				Utils.writeMatrix(c.getValues(), new Path(conf.get(inputMatrixCPathString)), peer
						.getConfiguration());
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
