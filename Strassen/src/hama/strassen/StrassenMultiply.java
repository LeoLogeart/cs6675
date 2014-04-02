package hama.strassen;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

public class StrassenMultiply {
	
	public static class StrassenBSP
	extends
	BSP<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> {
		
		private String path_a = "src/a";
		private String path_b = "src/b";
		private String path_c = "src/c";
		private int nbRows = 4;
		private int nbCols = 4;
		private int blockSize = 2;
		private static HashMap<Integer, Matrix> aBlocks;
		private static HashMap<Integer, Matrix> bBlocks;
		private HashMap<Integer, Matrix> resBlocks;
		
		@Override
		public void setup(BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException, SyncException, InterruptedException {
			if (peer.getPeerIndex()==0){
				aBlocks = new HashMap<Integer, Matrix>();
				bBlocks = new HashMap<Integer, Matrix>();
				Matrix a = new Matrix(Utils.readMatrix(new Path(path_a), peer.getConfiguration(), nbRows, nbCols),nbRows,nbCols);
				Matrix b = new Matrix(Utils.readMatrix(new Path(path_b), peer.getConfiguration(), nbRows, nbCols),nbRows,nbCols);
				int nbBlocks = (nbRows/blockSize)*(nbCols/blockSize);
				int peerInd = 0;
				for (int i=0;i<peer.getNumPeers()/nbBlocks;i++){
					Matrix aBlock = a.getBlock((int)Math.floor(i/2)*blockSize,(i%2)*blockSize,blockSize);
					Matrix bBlock= null;
					for (int j=0;j<peer.getNumPeers()/nbBlocks;j++){
						bBlock = b.getBlock((int)Math.floor(j/2)*blockSize,(j%2)*blockSize,blockSize);
						aBlocks.put(peerInd,aBlock);
						bBlocks.put(peerInd, bBlock);
						peerInd++;
					}
				}
			}
			peer.sync();
		}

		@Override
		public void bsp(
				BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException, SyncException, InterruptedException {
			Matrix aBlock = aBlocks.get(peer.getPeerIndex());
			Matrix bBlock = bBlocks.get(peer.getPeerIndex());
			Matrix resBlock = aBlock.strassen(bBlock);
			ResultMessage mes = new ResultMessage(peer.getPeerIndex(),resBlock);
			peer.send(peer.getPeerName(0),mes);
			peer.sync();	
		}
		
		@Override
		public void cleanup(BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException {
			if (peer.getPeerIndex()==0){
				ResultMessage result = peer.getCurrentMessage();
				while (result!=null){
					int peerInd = result.getSender();
					Matrix block = result.getResult();
					resBlocks.put(peerInd, block);
					result = peer.getCurrentMessage();
				}
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf);
		// Set the job name
		bsp.setJobName("Strassen Multiply");
		bsp.setBspClass(StrassenBSP.class);
		bsp.setOutputPath(new Path("src/out"));

		bsp.setNumBspTask(16);
		if (bsp.waitForCompletion(true)) {
			System.out.println("Job Finished");
		}

	}

}
