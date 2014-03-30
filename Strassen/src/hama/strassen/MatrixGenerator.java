package hama.strassen;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;

// Generates random matrix and stores it as sequence file in dfs
public class MatrixGenerator {

	public static void main(String[] args) throws InterruptedException,
			IOException, ClassNotFoundException {

		int rowSize = Integer.parseInt(args[0]);
		int colSize = Integer.parseInt(args[1]);
		Path outputPath = new Path(args[2]);

		double[][] matrix = createRandomMatrix(rowSize, colSize, new Random(),
				0, 100);

		Utils.writeMatrix(matrix, outputPath, new HamaConfiguration());
	}

	public static double[][] createRandomMatrix(int rows, int columns,
			Random rand, double rangeMin, double rangeMax) {

		double[][] matrix = new double[rows][columns];

		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < columns; j++) {
				double randomValue = rangeMin + (rangeMax - rangeMin)
						* rand.nextDouble();

				matrix[i][j] = new BigDecimal(randomValue).doubleValue();
			}
		}
		return matrix;
	}

}