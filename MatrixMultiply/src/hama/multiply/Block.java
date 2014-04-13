package hama.multiply;

public class Block {
	
	private int i;
	private int j;
	private Matrix block;
	
	public Block(int i, int j, int blockSize){
		this.i = i;
		this.j = j;
		this.block = new Matrix(blockSize,blockSize);
		block.zeroes();
	}

	public int getI() {
		return i;
	}

	public int getJ() {
		return j;
	}

	public Matrix getBlock() {
		return block;
	}
	
	public void setValue(int m, int n, double value){
		block.setValue(m, n, value);
	}
}
