package hama.strassen;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.bsp.BSP;
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
		private HashMap<Integer, Matrix> aBlocks;
		private HashMap<Integer, Matrix> bBlocks;
		private HashMap<Integer, Matrix> resBlocks;
		
		@Override
		public void setup(BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException, SyncException, InterruptedException {
			if (peer.getPeerIndex()==0){
				Matrix a = new Matrix(Utils.readMatrix(new Path(path_a), peer.getConfiguration(), nbRows, nbCols),nbRows,nbCols);
				Matrix b = new Matrix(Utils.readMatrix(new Path(path_b), peer.getConfiguration(), nbRows, nbCols),nbRows,nbCols);
				for (int i=0;i<peer.getNumPeers();i++){
					Matrix aBlock = a.getBlock(i*blockSize,i*blockSize,blockSize);
					Matrix bBlock = b.getBlock(i*blockSize,i*blockSize,blockSize);
					aBlocks.put(i, aBlock);
					bBlocks.put(i, bBlock);
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
			Matrix resBlock = aBlock.sum(bBlock);
			//TODO  Strassen
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

}
