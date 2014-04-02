package hama.strassen;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.io.VectorWritable;

public class StrassenMultiply {

	public static class StrassenBSP
			extends
			BSP<IntWritable, VectorWritable, IntWritable, VectorWritable, JobMessage> {
		
		private String path_a;
		private String path_b;
		private String path_c;
		private int nbRows;;
		private int nbCols;
		
		private static final int DONE_JOB = 0;
		private static final int DO_JOB = 1;
		private static final int END =2;
		
		Map<String,Integer> jobMasters = new HashMap<String, Integer>();

		//TODO SETUP to initialize attributes
		
		@Override
		public void bsp(
				BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, JobMessage> peer)
				throws IOException, SyncException, InterruptedException {
			Map<String,Map<String,Matrix>> doneJobs = new HashMap<String, Map<String,Matrix>>();
			if (peer.getPeerIndex()==0){
				HamaConfiguration conf = peer.getConfiguration();
				double[][] aValues = Utils.readMatrix(new Path(path_a), conf, nbRows, nbCols);
				double[][] bValues = Utils.readMatrix(new Path(path_b), conf, nbRows, nbCols);

				Matrix a = new Matrix(aValues, nbRows, nbCols);
				Matrix b = new Matrix(bValues, nbRows, nbCols);
				beginJob(a,b,"1",peer);
			}
			boolean finished = false;
			while(!finished){
				JobMessage mes = peer.getCurrentMessage();
				String jobId;
				switch(mes.getType()){
				case DO_JOB:
					Matrix m1 = mes.getFirstMatrix();
					Matrix m2 = mes.getSecondMatrix();
					jobId = mes.getJobId();
					int sender = mes.getSender();
					jobMasters.put(jobId,sender);
					beginJob(m1,m2,jobId,peer);
					break;
				case DONE_JOB:
					Matrix m = mes.getResult();
					jobId = mes.getJobId();
					String parentJob = jobId.substring(0,jobId.length()-1);
					if (doneJobs.get(parentJob)==null){
						doneJobs.put(parentJob, new HashMap<String, Matrix>());
					}
					doneJobs.get(parentJob).put(jobId,m);
					if (doneJobs.get(parentJob).size()==7){
						finishJob(parentJob,doneJobs,peer);
					}
					break;
				case END:
					finished = true;
					break;
				default:
					break;	
				}
			}
			peer.sync();
		}

		private void finishJob(String jobId, Map<String,Map<String,Matrix>> doneJobs, BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, JobMessage> peer) {
			try {
				Map<String,Matrix> jobs = doneJobs.get(jobId);
				Matrix[] m = new Matrix[7];
				for (int i=1;i<=7;i++){
					m[i-1] = jobs.get(jobId+i);
				}
				Matrix c11 = m[1].sum(m[4]).diff(m[5]).sum(m[7]);
				Matrix c12 = m[3].sum(m[5]);
				Matrix c21 = m[2].sum(m[4]);
				Matrix c22 = m[1].sum(m[2]).sum(m[3]).sum(m[6]);
				Matrix c = new Matrix(c11,c12,c21,c22);
				doneJobs.remove(jobId);
				if (jobId.equals("")){
					sendFinish(peer);
				} else {
					JobMessage doneJob = new JobMessage(c, jobId, DONE_JOB);
					String peerName = peer.getPeerName(jobMasters.get(jobId));
					peer.send(peerName,doneJob);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}

		private void beginJob(Matrix a, Matrix b, String jobId, BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, JobMessage> peer) {
			try {
				Integer slaveCounter = peer.getPeerIndex();
				if (a.size()==1){
					Matrix result = a.multiply(b);
					JobMessage doneJob = new JobMessage(result,jobId,DONE_JOB);
					String peerName = peer.getPeerName(jobMasters.get(jobId));
					peer.send(peerName,doneJob);
				} else {
					int peerIndex = peer.getPeerIndex();
					Matrix a11=null,a12=null,a21=null,a22=null,b11=null,b12=null,b21=null,b22=null;
					JobMessage doJob1 = new JobMessage(a11.sum(a22),b11.sum(b22),jobId+"1",DO_JOB,peerIndex);
					JobMessage doJob2 = new JobMessage(a21.sum(a22),b11,jobId+"2",DO_JOB,peerIndex);
					JobMessage doJob3 = new JobMessage(a11,b12.diff(b22),jobId+"3",DO_JOB,peerIndex);
					JobMessage doJob4 = new JobMessage(a22,b21.diff(b11),jobId+"4",DO_JOB,peerIndex);
					JobMessage doJob5 = new JobMessage(a11.sum(a12),b22,jobId+"5",DO_JOB,peerIndex);
					JobMessage doJob6 = new JobMessage(a21.diff(a11),b11.sum(b12),jobId+"6",DO_JOB,peerIndex);
					peer.send(getNextPeer(slaveCounter,peer), doJob1);
					peer.send(getNextPeer(slaveCounter,peer), doJob2);
					peer.send(getNextPeer(slaveCounter,peer), doJob3);
					peer.send(getNextPeer(slaveCounter,peer), doJob4);
					peer.send(getNextPeer(slaveCounter,peer), doJob5);
					peer.send(getNextPeer(slaveCounter,peer), doJob6);
					jobMasters.put(jobId+"7", peer.getPeerIndex());
					beginJob(a12.diff(a22), b21.sum(b22), jobId+"7", peer);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}

		private String getNextPeer(Integer slaveCounter,BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, JobMessage> peer) {
			slaveCounter = (slaveCounter+1)%peer.getNumPeers();
			if (slaveCounter==peer.getPeerIndex()){
				return getNextPeer(slaveCounter, peer);
			} else {
				return peer.getPeerName(slaveCounter);
			}
		}
		
		private void sendFinish(BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, JobMessage> peer){
			try {
				for (String peerName : peer.getAllPeerNames()){
					JobMessage end = new JobMessage(END);
					peer.send(peerName,end);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
