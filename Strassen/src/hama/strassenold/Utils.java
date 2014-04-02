package hama.strassenold;

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
	
	// Reads matrix from dfs
	public static double[][] readMatrix(Path path, HamaConfiguration conf, int rows, int columns){
		double[][] matrix = new double[rows][columns];
		SequenceFile.Reader reader = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			reader = new SequenceFile.Reader(fs, path, conf);
			VectorWritable row = new VectorWritable();
			IntWritable i = new IntWritable();
			while(reader.next(i,row)){
				DoubleVector v = row.getVector();
				for (int j=0;j<columns;j++){
					matrix[i.get()][j]=v.get(j);
				}
			}
		} catch (IOException e){
			e.printStackTrace();
		} finally {
			if (reader!=null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return matrix;
	}

}
