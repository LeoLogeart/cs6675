package utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

public class MatUtils {
	public static class NullBSP
			extends
			BSP<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> {

		@Override
		public void setup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
				throws IOException, SyncException, InterruptedException {
			peer.sync();
		}

		@Override
		public void bsp(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
				throws IOException, SyncException, InterruptedException {
			peer.sync();
		}

		@Override
		public void cleanup(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
				throws IOException {
		}
	}

	public static void main(String[] args) {
		if(args.length==0){
			printUsage();
			return;
		}
		if (args[0].equals("read")) {
			if(args.length!=4){
				printUsage();
				return;
			}
			double[][] cValues = Utils.readMatrix(new Path(args[1]),
					new HamaConfiguration(), Integer.parseInt(args[2]),
					Integer.parseInt(args[3]), 1);
			Matrix c = new Matrix(cValues, Integer.parseInt(args[2]),
					Integer.parseInt(args[3]));
			c.print();
		} else if (args[0].equals("read2")) {
			if(args.length!=4){
				printUsage();
				return;
			}
			double[][] cValues = Utils.readMatrix2(new Path(args[1]),
					new HamaConfiguration(), Integer.parseInt(args[2]),
					Integer.parseInt(args[3]), 1);
			Matrix c = new Matrix(cValues, Integer.parseInt(args[2]),
					Integer.parseInt(args[3]));
			c.print();
		} else if (args[0].equals("gen")) {
			if(args.length!=4){
				printUsage();
				return;
			}
			int rowSize = Integer.parseInt(args[1]);
			int colSize = Integer.parseInt(args[2]);
			Path outputPath = new Path(args[3]);

			double[][] matrix = MatrixGenerator.createRandomMatrix(rowSize,
					colSize, new Random(), 0, 100);
			MatrixGenerator.writeMatrix(matrix, outputPath,
					new HamaConfiguration());
		} else if (args[0].equals("gen2")) {
			if(args.length!=4){
				printUsage();
				return;
			}
			int rowSize = Integer.parseInt(args[1]);
			int colSize = Integer.parseInt(args[2]);
			Path outputPath = new Path(args[3]);

			double[][] matrix = MatrixGenerator.createRandomMatrix(rowSize,
					colSize, new Random(), 0, 100);
			MatrixGenerator.writeMatrix2(matrix, outputPath,
					new HamaConfiguration());
		}else if (args[0].equals("genBlock")) {
			if(args.length!=6){
				printUsage();
				return;
			}
			int rowSize = Integer.parseInt(args[1]);
			int colSize = Integer.parseInt(args[2]);
			int blockSize = Integer.parseInt(args[3]);
			String outputPath = args[4];
			String matrixName = args[5];
			int numBlockR = (int) Math.ceil((double)rowSize/(double)blockSize) ;
			int numBlockC = (int) Math.ceil((double)colSize/(double)blockSize);
			double[][] matrix;
			for(int i = 0 ;i<numBlockR;i++){
				for(int j=0;j<numBlockC;j++){
					matrix = MatrixGenerator.createRandomMatrix(blockSize,
							blockSize, new Random(), 0, 100);
					MatrixGenerator.writeMatrix(matrix, new Path(outputPath+"/"+matrixName+"/"+matrixName+i+"_"+j+".mat"),
									new HamaConfiguration());
				}
			}
			 
			
		}else if (args[0].equals("cmp")) {
			if(args.length!=5){
				printUsage();
				return;
			}
			int rows = Integer.parseInt(args[3]);
			double[][] aValues = Utils
					.readMatrix(new Path(args[1]), new HamaConfiguration(),
							rows, Integer.parseInt(args[4]), 1);

			double[][] bValues = Utils
					.readMatrix(new Path(args[2]), new HamaConfiguration(),
							rows, Integer.parseInt(args[4]), 1);
			boolean eq = true;
			int i = 0;
			while (i < rows && eq) {
				eq=Arrays.equals(aValues[i], bValues[i]);
				i++;
			}
			if (eq) {
				System.out.println("Matrices match");
			} else {
				System.out.println("Matrices do not match");
			}
		} else if (args[0].equals("hamaSetup")) {
			if(args.length!=2){
				printUsage();
				return;
			}
			HamaConfiguration conf = new HamaConfiguration();
			BSPJob bsp;
			try {
				bsp = new BSPJob(conf);

				// Set the job name
				bsp.setJobName("Empty job");
				bsp.setBspClass(NullBSP.class);
				bsp.setJar("matUtils.jar");
				bsp.setOutputPath(new Path("tmp"));
				bsp.setNumBspTask(Integer.parseInt(args[1]));

				long startTime = System.currentTimeMillis();
				if (bsp.waitForCompletion(true)) {
					System.out.println("Running Hama took "
							+ (System.currentTimeMillis() - startTime) / 1000.0
							+ " seconds.");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			printUsage();
		}
	}

	private static void printUsage() {
		System.out.println("Usage :");
		System.out
				.println("hama -jar utils.jar read <path to matrix> <rows size> <column size>");
		System.out
				.println("hama -jar utils.jar gen <row size> <column size> <output path>");
		System.out.println("hama -jar utils.jar genBlock <row size> <column size> <block size> <output path> <matrix name>");
		System.out
				.println("hama -jar utils.jar cmp <path matrix 1> <path matrix 2> <mat rows size> <mat column size>");
		System.out.println("hama -jar utils.jar hamaSetup <number of nodes>");
	}

}
