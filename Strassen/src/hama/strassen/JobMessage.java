package hama.strassen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JobMessage implements Writable{
	
	private int type; //O for job 1 for res
	private int i;
	private int j;
	private int k;
	
	private int sender;
	private Matrix result;

	public JobMessage() {
		super();
	}

	public JobMessage(int type, int i, int j, int k) {
		super();
		this.type = type;
		this.i = i;
		this.j = j;
		this.k = k;
	}
	
	public JobMessage(int type,int sender, Matrix result) {
		this.type = type;
		this.sender = sender;
		this.result = result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		if (type==0) {
			i = in.readInt();
			j = in.readInt();
			k = in.readInt();
		} else if (type==1){
			sender = in.readInt();
			result = DoubleMatrixWritable.read(in);
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (type==0){
			out.writeInt(i);
			out.writeInt(j);
			out.writeInt(k);
		} else if (type==1){
			out.writeInt(sender);
			DoubleMatrixWritable.write(result, out);
		}
		
	}
	
	public int getSender() {
		return sender;
	}

	public Matrix getResult() {
		return result;
	}


	public int getI() {
		return i;
	}

	public int getJ() {
		return j;
	}

	public int getK() {
		return k;
	}
}
