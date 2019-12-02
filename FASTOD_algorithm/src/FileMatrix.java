import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileMatrix {
	private String [][] mat;
	private int nr;
	private int nc;
	
	FileMatrix(int nrows, int ncols){
		nr = nrows;
		nc = ncols;
		mat = new String[nrows][ncols];
	}
	
	FileMatrix(){
		mat = new String[30000][10000];
	}
	
	public void insertIntoMatrix(String s, int row, int col){
		mat[row][col]= s;
	}
	
	public void printMatrix() {
		int i,j;
		for(i=0; i<nr; i++){
			for(j=0; j<nc; j++){
				System.out.print(mat[i][j] + " ");
			}
			System.out.println();
		}	
	}
	
	public String[] getColumn(int j) {
		int i;
		String[] col = new String[nr];
		for(i=0; i<nr; i++){
				col[i]=mat[i][j];
				//System.out.println(col[i]);
		}	
	return col;
	}
	
	public int getNumCols() {
		return nc;
	}
	
	public int getNumRows() {
		return nr;
	}
	
	public String getElement(int row, int col) {
		return mat[row][col];
	}
	
	public String[] insertByCSVFile(String url){
		BufferedReader br = null;
		String[] columnNames=null;
		String line = "";
		String cvsSplitBy = ",";
		int r=0;
		int c=0;
		try {
			br = new BufferedReader(new FileReader(url));
			line = br.readLine();
			columnNames = line.split(cvsSplitBy);
			while ((line = br.readLine()) != null) {
		        // use comma as separator
				String[] row = line.split(cvsSplitBy);
				for(int i=0; i<row.length; i++) {
					mat[r][i]= row[i];
				}
				r++;
				c=row.length;
		}
		this.nr=r;
		this.nc=c;
		//System.out.println("num righe " +r+ " numcolonne " +c);
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	//System.out.println("Done");
	return columnNames;
  }
	
}
