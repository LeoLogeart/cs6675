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

	public Matrix sum(Matrix a22) {
		Matrix c = a22;
		for (int i = 0; i < nbRows; i++) {
			for (int j = 0; j < nbCols; j++) {
				c.setValue(i, j, c.get(i, j) + this.get(i, j));
			}
		}
		return c;
	}

	public Matrix diff(Matrix a22) {
		Matrix c = a22;
		for (int i = 0; i < nbRows; i++) {
			for (int j = 0; j < nbCols; j++) {
				c.setValue(i, j, this.get(i, j) - c.get(i, j));
			}
		}
		return null;
	}

	public void setValue(int i, int j, double value) {
		values[i][j] = value;
	}

	public Matrix get11() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i = 0; i < nbRows / 2; i++) {
			m[i] = Arrays.copyOfRange(values[i], 0, nbCols / 2-1);// TODO
																// nbRows/2-1?
		}
		Matrix m11 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m11;
	}

	public Matrix get12() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i = 0; i < nbRows / 2; i++) {
			m[i] = Arrays.copyOfRange(values[i], nbCols / 2, nbCols-1);// TODO nbRows/2-1?
		}
		Matrix m11 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m11;
	}

	public Matrix get21() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i =  nbRows / 2; i < nbRows ; i++) {
			m[i-nbRows/2] = Arrays.copyOfRange(values[i], 0, nbCols / 2 -1);// TODO nbRows/2-1?
		}
		Matrix m11 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m11;
	}

	public Matrix get22() {
		double[][] m = new double[nbRows / 2][nbCols / 2];
		for (int i =  nbRows / 2; i < nbRows ; i++) {
			m[i-nbRows/2] = Arrays.copyOfRange(values[i], nbCols / 2, nbCols-1);// TODO nbRows/2-1?
		}
		Matrix m11 = new Matrix(m, nbRows / 2, nbCols / 2);
		return m11;
	}

	public void set(int row, int col, double readDouble) {
		values[row][col]=readDouble;
		
	}
}
