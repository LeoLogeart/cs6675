package hama.strassen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JobMessage implements Writable{
	
	private MessageType type;
	
	public JobMessage(Matrix result, String jobId, MessageType doneJob) {
		// TODO Auto-generated constructor stub
	}

	public JobMessage(Matrix sum, Matrix sum2, String string) {
		// TODO Auto-generated constructor stub
	}

	public MessageType getType(){
		return type;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	public enum MessageType {DO_JOB,DONE_JOB,END}

	public Matrix getFirstMatrix() {
		// TODO Auto-generated method stub
		return null;
	}

	public Matrix getSecondMatrix() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getJobId() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getSender() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Matrix getResult() {
		// TODO Auto-generated method stub
		return null;
	};

}


