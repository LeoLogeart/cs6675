package hama.strassen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.io.VectorWritable;

public class StrassenMultiply {

	private static class StrassenBSP
			extends
			BSP<IntWritable, VectorWritable, IntWritable, VectorWritable, NullWritable> {

		@Override
		public void bsp(
				BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, NullWritable> arg0)
				throws IOException, SyncException, InterruptedException {
			// TODO Auto-generated method stub

		}

	}

}
