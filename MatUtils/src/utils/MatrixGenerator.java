package utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleVector;

// Generates random matrix and stores it as sequence file in dfs
public class MatrixGenerator {

	public static void main(String[] args) throws InterruptedException,
			IOException, ClassNotFoundException {

		int rowSize = Integer.parseInt(args[0]);
		int colSize = Integer.parseInt(args[1]);
		Path outputPath = new Path(args[2]);

		double[][] matrix = createRandomMatrix(rowSize, colSize, new Random(),
				0, 100);

		// double[][] a={{1,2,3},{2,0,1},{1,1,1},{2,2,0}};
		// double[][] b={{1,2},{0,1},{1,1}};
		writeMatrix(matrix, outputPath, new HamaConfiguration());
	}

	public static double[][] createRandomMatrix(int rows, int columns,
			Random rand, double rangeMin, double rangeMax) {

		double[][] matrix = new double[rows][columns];

		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < columns; j++) {
				double randomValue = rangeMin + (rangeMax - rangeMin)
						* rand.nextDouble();

				matrix[i][j] = randomValue;
			}
		}
		return matrix;
	}

	// Stores matrix as sequence file in dfs
	public static Path writeMatrix(double[][] matrix, Path path,
			HamaConfiguration conf) {
		SequenceFile.Writer writer = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class,
					VectorWritable.class);
			for (int i = 0; i < matrix.length; i++) {
				DenseDoubleVector rowVector = new DenseDoubleVector(matrix[i]);
				writer.append(new IntWritable(i), new VectorWritable(rowVector));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return path;
	}

	public static void writeMatrix2(double[][] matrix, Path path,
			HamaConfiguration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
					fs.create(path, true)));
			for (int j = 0; j < matrix[0].length; j++) {
				for (int i = 0; i < matrix.length; i++) {
					br.write(String.valueOf(matrix[i][j])+",");
				}
				br.write("\n");
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}