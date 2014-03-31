package hama.strassen;

public class Matrix {
	
	private double[][] values;
	private int nbRows;
	private int nbCols;
	
	public Matrix(double[][] values, int nbRows, int nbCols){
		this.values = values;
		this.nbRows = nbRows;
		this.nbCols = nbCols;
	}
	
	public Matrix(Matrix c11, Matrix c12, Matrix c21, Matrix c22) {
		// TODO Auto-generated constructor stub
	}

	public double get(int i, int j){
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

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Matrix multiply(Matrix b) {
		// TODO Auto-generated method stub
		return null;
	}

	public Matrix sum(Matrix a22) {
		// TODO Auto-generated method stub
		return null;
	}

	public Matrix diff(Matrix a22) {
		// TODO Auto-generated method stub
		return null;
	}
}
