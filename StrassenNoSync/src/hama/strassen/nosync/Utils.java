package hama.strassen.nosync;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;

public class Utils {

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

	// Reads matrix from dfs and pads it to have rows and columns dividable by
	// blocksize
	public static double[][] readMatrix(Path path, HamaConfiguration conf,
			int rows, int columns, int blockSize) {

		int finalRows = getBlockMultiple(rows, blockSize);
		int finalCols = getBlockMultiple(columns, blockSize);

		double[][] matrix = new double[finalRows][finalCols];
		SequenceFile.Reader reader = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			reader = new SequenceFile.Reader(fs, path, conf);
			VectorWritable row = new VectorWritable();
			IntWritable i = new IntWritable();
			while (reader.next(i, row) && i.get()<rows) {
				DoubleVector v = row.getVector();
				for (int j = 0; j < columns; j++) {
					matrix[i.get()][j] = v.get(j);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		for (int k = rows; k < finalRows; k++) {
			for (int p = columns; p < finalCols; p++) {
				matrix[k][p] = 0;
			}
		}
		return matrix;
	}

	public static int getBlockMultiple(int size, int blockSize) {
		int res = size;
		if (res % blockSize != 0) {
			res = res + blockSize - (res % blockSize);
		}
		return res;
	}

}
