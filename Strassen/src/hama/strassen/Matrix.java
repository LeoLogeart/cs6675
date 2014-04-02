package hama.strassen;

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
		for (int i = c11.getNbRows(); i < nbRows; i++) {
			for (int j = 0; j < c21.getNbCols(); j++) {
				values[i][j] = c21.get(i, j);
			}
		}
		for (int i = 0; i < c12.getNbRows(); i++) {
			for (int j = c11.getNbCols(); j < nbCols; j++) {
				values[i][j] = c11.get(i, j);
			}
		}
		for (int i = c12.getNbRows(); i < nbRows; i++) {
			for (int j = c21.getNbCols(); j < nbCols; j++) {
				values[i][j] = c11.get(i, j);
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
		this.values=a.getValues();
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

	public Matrix multiply(Matrix b) {
		double m[][] = { { this.get(0, 0) * b.get(0, 0) } };
		return new Matrix(m, 1, 1);
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

	public void set(int row, int col, double readDouble) {
		values[row][col]=readDouble;
	}
	
	public void print(){
		for (int i=0;i<nbRows;i++){
			for (int j=0;j<nbCols;j++){
				System.out.print(values[i][j]+",");
			}
			System.out.println();
		}
	}

	public Matrix getBlock(int i, int j, int blockSize) {
		// TODO Get block starting at (i,j) of size blockSize
		return null;
	}

	public Matrix strassen(Matrix b) {
		if (b.getNbRows() == 1) {
			double[][] val = { { get(0, 0) * b.get(0, 0) } };
			return new Matrix(val, 1, 1);
		} else {
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

			return new Matrix(c11, c12, c21, c22);
		}
	}
}
