package hama.strassen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ResultMessage implements Writable{
	
	private int sender;
	private Matrix result;

	public ResultMessage(int sender, Matrix result) {
		this.sender = sender;
		this.result = result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sender = in.readInt();
		result = DoubleMatrixWritable.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(sender);
		DoubleMatrixWritable.write(result, out);	
	}

	public int getSender() {
		return sender;
	}

	public Matrix getResult() {
		return result;
	}

}