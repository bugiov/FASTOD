import de.metanome.algorithm_integration.AlgorithmExecutionException;
import java.io.IOException;

public class OD_Discovery {

	public static void main(String[] args) throws IOException {
				
                                if(args[0].isEmpty()){
                                    System.out.println("Dataset mancante!");
                                    return;
                                }
                                
				System.out.println("ESEGUO FASTOD\n\n");
                                
				FastodAlgorithm fastod = new FastodAlgorithm(args[0]);
				try {
					fastod.execute();
				} catch (AlgorithmExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

	}

}
