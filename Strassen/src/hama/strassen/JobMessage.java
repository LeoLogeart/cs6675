package hama.strassen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JobMessage implements Writable{
	
	private int type;
	private Matrix result;
	private String jobId;
	private Matrix firstMatrix;
	private Matrix secondMatrix;
	private int sender;
	
	public JobMessage(Matrix result, String jobId, int type) {
		this.result = result;
		this.jobId = jobId;
		this.type = type;
	}

	public JobMessage(Matrix firstMatrix, Matrix secondMatrix, String jobId, int type, int sender) {
		this.firstMatrix = firstMatrix;
		this.secondMatrix = secondMatrix;
		this.jobId = jobId;
		this.type = type;
		this.sender = sender;
	}

	public JobMessage(int type) {
		this.type = type;
	}

	public int getType(){
		return type;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		switch (type){
		case 0:
			result = DoubleMatrixWritable.read(in);
			jobId = in.readLine();
			break;
		case 1:
			firstMatrix = DoubleMatrixWritable.read(in);
			secondMatrix = DoubleMatrixWritable.read(in);
			sender = in.readInt();
			jobId = in.readLine();
			break;
		case 2:
			break;
		default :
				break;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(type);
		switch (type){
		case 0:
			DoubleMatrixWritable.write(result, out);
			out.writeBytes(jobId);
			break;
		case 1:
			DoubleMatrixWritable.write(firstMatrix, out);
			DoubleMatrixWritable.write(secondMatrix, out);
			out.write(sender);
			out.writeBytes(jobId);
			break;
		case 2:
			break;
		default :
				break;
		}
		
	}

	public Matrix getFirstMatrix() {
		return firstMatrix;
	}

	public Matrix getSecondMatrix() {
		return secondMatrix;
	}

	public String getJobId() {
		return jobId;
	}

	public int getSender() {
		return sender;
	}

	public Matrix getResult() {
		return result;
	};

}


