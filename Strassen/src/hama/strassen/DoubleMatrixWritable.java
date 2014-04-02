package hama.strassen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DoubleMatrixWritable implements Writable {

	private Matrix mat;

	@Override
	public void readFields(DataInput in) throws IOException {
		mat = read(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		write(mat, out);

	}

	public static void write(Matrix mat, DataOutput out) throws IOException {
		out.writeInt(mat.getNbRows());
	    out.writeInt(mat.getNbCols());
	    for (int row = 0; row < mat.getNbRows(); row++) {
	      for (int col = 0; col < mat.getNbCols(); col++) {
	        out.writeDouble(mat.get(row, col));
	      }
	    }
	}

	public static Matrix read(DataInput in) throws IOException {
		Matrix mat = new Matrix(in.readInt(), in.readInt());
		for (int row = 0; row < mat.getNbRows(); row++) {
			for (int col = 0; col < mat.getNbCols(); col++) {
				mat.setValue(row, col, in.readDouble());
			}
		}
		return mat;
	}

}
