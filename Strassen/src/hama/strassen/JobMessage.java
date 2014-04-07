package hama.strassen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JobMessage implements Writable{
	
	private int i;
	private int j;
	private int k;
	
	

	public int getI() {
		return i;
	}

	public int getJ() {
		return j;
	}

	public int getK() {
		return k;
	}

	public JobMessage() {
		super();
	}

	public JobMessage(int i, int j, int k) {
		super();
		this.i = i;
		this.j = j;
		this.k = k;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		j = in.readInt();
		k = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeInt(j);
		out.writeInt(k);
	}

}
