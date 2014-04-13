package hama.strassen.nosync;

import java.util.Arrays;

public class Matrix{

	private double[][] values;
	private int nbRows;
	private int nbCols;

	public Matrix(double[][] values, int nbRows, int nbCols) {
		this.values = values;
		this.nbRows = nbRows;
		this.nbCols = nbCols;
	}

	public Matrix(Matrix c11, Matrix c12, Matrix c21, Matrix c22) {
		nbRows = c11.getNbRows() + c12.getNbRows();
		nbCols = c11.getNbCols() + c21.getNbCols();
		values = new double[nbRows][nbCols];

		for (int i = 0; i < c11.getNbRows(); i++) {
			for (int j = 0; j < c11.getNbCols(); j++) {
				values[i][j] = c11.get(i, j);
			}
		}
		for (int i = c21.getNbRows(); i < nbRows; i++) {
			for (int j = 0; j < c21.getNbCols(); j++) {
				values[i][j] = c21.get(i-c21.getNbRows(), j);
			}
		}
		for (int i = 0; i < c12.getNbRows(); i++) {
			for (int j = c12.getNbCols(); j < nbCols; j++) {
				values[i][j] = c12.get(i, j-c12.getNbCols());
			}
		}
		for (int i = c22.getNbRows(); i < nbRows; i++) {
			for (int j = c22.getNbCols(); j < nbCols; j++) {
				values[i][j] = c22.get(i-c22.getNbRows(), j-c22.getNbCols());
			}
		}
	}
	
	public Matrix(Matrix[][] blocks,int nbRows, int nbCols, int blockSize){
		this.nbRows = nbRows;
		this.nbCols = nbCols;
		values = new double[nbRows][nbCols];
		for (int i=0;i<blocks.length;i++){
			for (int j=0;j<blocks[i].length;j++){
				for (int k=i*blockSize;k<(i+1)*blockSize;k++){
					for (int m=j*blockSize;m<(j+1)*blockSize;m++){
						values[k][m]=blocks[i][j].get(k%blockSize,m%blockSize);
					}
				}
			}
		}
	}

	public Matrix(int nbRows, int nbCols) {
		values = new double[nbRows][nbCols];
		this.nbRows = nbRows;
		this.nbCols = nbCols;
	}

	public Matrix(Matrix a) {
		this.nbCols=a.getNbCols();
		this.nbRows=a.getNbRows();
		double[][] m = new double[nbRows][nbCols];
		for (int i = 0 ; i < nbRows ; i++) {
			m[i] = Arrays.copyOfRange(a.getValues()[i], 0, nbCols);
		}
		this.values=m;
	}

	public double get(int i, int j) {
		return values[i][j];
	}

	public double[][] getValues() {
		return values;
	}

	public int getNbRows() {
		return nbRows;
	}

	public int getNbCols() {
		return nbCols;
	}

	/**
	 * True only if square matrix
	 * 
	 * @return
	 */
	public int size() {
		return nbCols;
	}

	public Matrix sum(Matrix a) {
		Matrix c = new Matrix(a);
		
		for (int i = 0; i < nbRows; i++) {
			for (int j = 0; j < nbCols; j++) {
				c.setValue(i, j, c.get(i, j) + this.get(i, j));
			}
		}
		return c;
	}

	public Matrix diff(Matrix a) {
		Matrix c = new Matrix(a);
		for (int i = 0; i < nbRows; i++) {
			for (int j = 0; j < nbCols; j++) {
				c.setValue(i, j, this.get(i, j) - c.get(i, j));
			}
		}
		return c;
	}

	public void setValue(int i, int j, double value) {
		values[i][j] = value;
	}

	public Matrix get11() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i = 0; i < nbRows / 2; i++) {
			m[i] = Arrays.copyOfRange(values[i], 0, nbCols / 2);// TODO
																// nbRows/2-1?
		}
		Matrix m11 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m11;
	}

	public Matrix get12() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i = 0; i < nbRows / 2; i++) {
			m[i] = Arrays.copyOfRange(values[i], nbCols / 2, nbCols);// TODO nbRows/2-1?
		}
		Matrix m12 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m12;
	}

	public Matrix get21() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i =  nbRows / 2; i < nbRows ; i++) {
			m[i-nbRows/2] = Arrays.copyOfRange(values[i], 0, nbCols / 2 );// TODO nbRows/2-1?
		}
		Matrix m21 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m21;
	}

	public Matrix get22() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i =  nbRows / 2; i < nbRows ; i++) {
			m[i-nbRows/2] = Arrays.copyOfRange(values[i], nbCols / 2, nbCols);// TODO nbRows/2-1?
		}
		Matrix m22 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m22;
	}
	
	public void print(){
		System.out.println(toString());
	}
	
	public Matrix getBlock(int iInd, int jInd, int blockSize){
		Matrix block = new Matrix(blockSize,blockSize);
		for (int i=iInd*blockSize;i<iInd*blockSize+blockSize;i++){
			for (int j=jInd*blockSize;j<jInd*blockSize+blockSize;j++){
				block.setValue(i%blockSize, j%blockSize, values[i][j]);
			}
		}
		return block;
	}

	public Matrix strassen(Matrix b) {
		if (b.getNbRows() <= 32) {
			return mult(b);
		} else {
			long start = System.currentTimeMillis();
			Matrix m1 = get11().sum(get22()).strassen(b.get11().sum(b.get22()));
			Matrix m2 = get21().sum(get22()).strassen(b.get11());
			Matrix m3 = get11().strassen(b.get12().diff(b.get22()));
			Matrix m4 = get22().strassen(b.get21().diff(b.get11()));
			Matrix m5 = get11().sum(get12()).strassen(b.get22());
			Matrix m6 = get21().diff(get11())
					.strassen(b.get11().sum(b.get12()));
			Matrix m7 = get12().diff(get22())
					.strassen(b.get21().sum(b.get22()));

			Matrix c11 = m1.sum(m4).diff(m5).sum(m7);
			Matrix c12 = m3.sum(m5);
			Matrix c21 = m2.sum(m4);
			Matrix c22 = m1.diff(m2).sum(m3).sum(m6);

			Matrix c = new Matrix(c11, c12, c21, c22);
			return c;
		}
	}
	
	public Matrix mult(Matrix b) {
		double[][] C = new double[getNbRows()][b.getNbCols()];

		for (int i = 0; i < getNbRows(); i++) { // aRow
			for (int j = 0; j < b.getNbCols(); j++) { // bColumn
				for (int k = 0; k < getNbCols(); k++) { // aColumn
					C[i][j] += get(i, k) * b.get(k, j);
				}
			}
		}

		return new Matrix(C, getNbRows(), b.getNbCols());
	}
	
	public String toString(){
		StringBuilder sb= new StringBuilder();
		for (int i=0;i<nbRows;i++){
			for (int j=0;j<nbCols;j++){
				sb.append(values[i][j]+",");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	public void zeroes() {
		for (int i=0;i<nbRows;i++){
			for (int j=0;j<nbCols;j++){
				values[i][j]=0;
			}
		}
		
	}
}
