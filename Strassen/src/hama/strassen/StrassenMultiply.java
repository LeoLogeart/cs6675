package hama.strassen;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
		private static int nbRows = 4;
		private static int nbCols = 4;
		private static int blockSize = 2;
		private static HashMap<Integer, Matrix> aBlocks;
		private static HashMap<Integer, Matrix> bBlocks;
		private static HashMap<Integer, Matrix> resBlocks;
		private static HashMap<Integer,Integer[]> indices;
		
		@Override
		public void setup(BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, ResultMessage> peer)
				throws IOException, SyncException, InterruptedException {
			if (peer.getPeerIndex()==0){
				aBlocks = new HashMap<Integer, Matrix>();
				bBlocks = new HashMap<Integer, Matrix>();
				Matrix a = new Matrix(Utils.readMatrix(new Path(path_a), peer.getConfiguration(), nbRows, nbCols),nbRows,nbCols);
				Matrix b = new Matrix(Utils.readMatrix(new Path(path_b), peer.getConfiguration(), nbRows, nbCols),nbRows,nbCols);
				System.out.println("A*B");
				System.out.println(a.mult(b).toString());
				//int nbBlocks = (nbRows/blockSize)*(nbCols/blockSize);
				int peerInd = 0;
				indices = new HashMap<Integer, Integer[]>();
				for (int i=0;i<nbRows/blockSize;i++){
					for (int j=0;j<nbCols/blockSize;j++){
						for (int k=0;k<blockSize;k++){
							Matrix aBlock = a.getBlock(i,k,blockSize);
							Matrix bBlock = b.getBlock(k, j, blockSize);	
							aBlocks.put(peerInd,aBlock);
							bBlocks.put(peerInd, bBlock);
							indices.put(peerInd, new Integer[]{i,j});
							peerInd++;
						}
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
				resBlocks= new HashMap<Integer, Matrix>();
				ResultMessage result = peer.getCurrentMessage();
				while (result!=null){
					int peerInd = result.getSender();
					Matrix block = result.getResult();
					resBlocks.put(peerInd, block);
					result = peer.getCurrentMessage();
				}
				
				Matrix[][] cBlocks = new Matrix[nbRows/blockSize][nbCols/blockSize];
				for (int i=0;i<nbRows/blockSize;i++){
					for (int j=0;j<nbCols/blockSize;j++){
						cBlocks[i][j] = new Matrix(blockSize,blockSize);
						cBlocks[i][j].zeroes();
					}
				}
				for (Integer peerInd : indices.keySet()){
					Integer[] inds = indices.get(peerInd);
					cBlocks[inds[0]][inds[1]] = cBlocks[inds[0]][inds[1]].sum(resBlocks.get(peerInd));
				}
				
				System.out.println("C Blocks");
				for (int i=0;i<nbRows/blockSize;i++){
					for (int j=0;j<nbCols/blockSize;j++){
						cBlocks[i][j].print();
					}
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

		bsp.setNumBspTask(8);
		if (bsp.waitForCompletion(true)) {
			System.out.println("Job Finished");
		}

	}

}
