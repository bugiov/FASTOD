import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.OpenBitSet;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.algorithm_types.FunctionalDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FastodAlgorithm implements FunctionalDependencyAlgorithm,
        RelationalInputParameterAlgorithm,
        StringParameterAlgorithm {

    public static final String INPUT_SQL_CONNECTION = "DatabaseConnection";
    public static final String INPUT_TABLE_NAME = "Table_Name";
    public static final String INPUT_TAG = "Relational_Input";

    private DatabaseConnectionGenerator databaseConnectionGenerator;
    private RelationalInputGenerator relationalInputGenerator;
    private String tableName;
    private int numberAttributes;
    private long numberTuples;
    private List<String> columnNames;
    private ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    private Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level0 = null;
    private Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> level1 = null;
    private Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>> prefix_blocks = null;
    private LongBigArrayBigList tTable;
    private long tempoStart;
    public String[] helpname;
    FileMatrix datafile;
    public int nc, nr;
    private static int numFD;
    private static int numOCD;
    private static String datasetFile;
    private static PrintWriter log;
    private static PrintWriter out;
    
    public FastodAlgorithm(String dataset) throws IOException{
        datasetFile = dataset;
        //creo file per output e log
        String nameFile = datasetFile.substring(0, datasetFile.length()-4);
        FileWriter fLog;
        FileWriter fOut;
        fLog = new FileWriter("./Log_" +nameFile +".txt", true);
        fOut = new FileWriter("./Output_" +nameFile +".txt", true);
        log = new PrintWriter(fLog);
        out = new PrintWriter(fOut);
    }
    
    ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions;

    @Override
    public ArrayList<ConfigurationRequirement> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement> requiredConfig = new ArrayList<ConfigurationRequirement>();
//		requiredConfig.add(new ConfigurationSpecificationSQLIterator(INPUT_SQL_CONNECTION));
        requiredConfig.add(new ConfigurationRequirementRelationalInput(INPUT_TAG));
//		requiredConfig.add(new ConfigurationSpecificationString(INPUT_TABLE_NAME));

        return requiredConfig;
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
        if (identifier.equals(INPUT_TABLE_NAME)) {
            this.tableName = values[0];
        }
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
        if (identifier.equals(INPUT_TAG)) {
            this.relationalInputGenerator = values[0];
        }
    }

    @Override
    public void setResultReceiver(
            FunctionalDependencyResultReceiver resultReceiver) {
        //this.fdResultReceiver = resultReceiver;
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
    	System.out.println("Eseguo");
    	tempoStart = System.currentTimeMillis();
        
    	numFD=0;
        numOCD=0;
        
        level0 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        level1 = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();
        prefix_blocks = new Object2ObjectOpenHashMap<OpenBitSet, ObjectArrayList<OpenBitSet>>();

        //Get information about table from database or csv file
        partitions = loadData();
        
        setColumnIdentifiers();
        numberAttributes = this.columnNames.size();

        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        // Initialize Level 0   L0={}
        CombinationHelper chLevel0 = new CombinationHelper();//
        OpenBitSet rhsCandidatesLevel0 = new OpenBitSet();//
        OpenBitSet rhsCandidates_SLevel0 = new OpenBitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);//Cc+({}) = R
        //Cs+({}) = {}
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);//combinationHelper.setRhsCandidates
        chLevel0.setRhsCandidatesS(rhsCandidates_SLevel0);//combinationHelper.setRhsCandidates
        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);//
        chLevel0.setPartition(spLevel0); //setta partizione livello 0 al combination helper
        spLevel0 = null;    //strippedPartition = null
        level0.put(new OpenBitSet(), chLevel0); //inserisce nel livello 0 un openbitset vuoto {} e il ch per level 0
        chLevel0 = null;    //combinationHelperl0
      
        int l = 1;  //l=1    
        
        //Initialize Level 1   L1={{A}|A € R}
        for (int i = 1; i <= numberAttributes; i++) {
            CombinationHelper chLevel1 = new CombinationHelper();//
            OpenBitSet rhsCandidatesLevel1 = new OpenBitSet();//
            OpenBitSet rhsCandidates_SLevel1 = new OpenBitSet();//
            rhsCandidatesLevel1.set(1, numberAttributes + 1);//bitset.set, TUTTI gli attributi
            //Cs+={}
            chLevel1.setRhsCandidates(rhsCandidatesLevel1);
            chLevel1.setRhsCandidatesS(rhsCandidates_SLevel1);
            StrippedPartition spLevel1 = new StrippedPartition(partitions.get(i - 1));//prende la partizione per l'attributo i-esimo (arraylist, -1)
            chLevel1.setPartition(spLevel1);//setta partizione del chLevel_1
            OpenBitSet combinationLevel1 = new OpenBitSet();
            combinationLevel1.set(i);   //Setta bit attributo A
            //inserimento in livello 1 dell'attributo considerato (V A€R)
            level1.put(combinationLevel1, chLevel1);    //key = idAttributo, value=CH_attributo
        }
        
        //partitions = null;
        
        while (!level1.isEmpty() && l <= numberAttributes) {    //while(Li!={}) do:
            // compute dependencies for a level
            computeODs(l);    //computeODs

            // pruneLevels the search space
            pruneLevels(l);  //pruneLevels

            // compute the combinations for the next level 
            calculateNextLevel(l);        //calculateNextLevel()
            l++;
        }
        /*
        System.out.println("");
        System.out.println("Sono state trovave "+numFD+" dipendenze di ordinamento COSTANTI");
        System.out.println("Sono state trovave "+numOCD+" dipendenze di ordinamento CANONICHE");
        */
        //stampa su file log
        log.println("");
        log.println("Sono state trovave "+numFD+" dipendenze di ordinamento COSTANTI");
        log.println("Sono state trovave "+numOCD+" dipendenze di ordinamento CANONICHE");
        stampaTempo();
        log.print("secondi");
        
        //chiudo file
        out.close();
        log.close();
    }

    private ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> loadData() throws InputGenerationException, InputIterationException {
        //ritorna le partizioni
        RelationalInput input = null;
        //System.out.println("1");
        //System.out.println("relInpGen: "+this.relationalInputGenerator+" fine");
        //System.out.println("DBConn: "+this.databaseConnectionGenerator+" fine");
        if (this.relationalInputGenerator != null) {
        	//System.out.println("1");
            input = this.relationalInputGenerator.generateNewCopy();
        } else if (this.databaseConnectionGenerator != null) {
        	//System.out.println("2");
            String sql = "SELECT * FROM " + this.tableName;
            input = this.databaseConnectionGenerator.generateRelationalInputFromSql(sql);
        } 
        if (input != null) {
            this.numberAttributes = input.numberOfColumns();
            this.tableName = input.relationName();
            this.columnNames = input.columnNames();
            ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(this.numberAttributes);
            for (int i = 0; i < this.numberAttributes; i++) {
                Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<Object, LongBigArrayBigList>();
                partitions.add(partition);
            }
            long tupleId = 0;
            while (input.hasNext()) {
                List<String> row = input.next();
                for (int i = 0; i < this.numberAttributes; i++) {
                    Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = partitions.get(i);
                    String entry = row.get(i);
                    if (partition.containsKey(entry)) {
                        partition.get(entry).add(tupleId);
                    } else {
                        LongBigArrayBigList newEqClass = new LongBigArrayBigList();
                        newEqClass.add(tupleId);
                        partition.put(entry, newEqClass);
                    }
                    ;
                }
                tupleId++;
            }
            this.numberTuples = tupleId;
            return partitions;
        }
        else {//input da file
        	//CREO LE MATRICI DI DIFFERENZA
        	//columnNames = new ArrayList<String>();
                //String path = "C:/table1.csv";
                String path = "./";
        	path += datasetFile;
        	datafile = new FileMatrix();
    		helpname = datafile.insertByCSVFile(path);
    		
        	BufferedReader br = null;
    		String line = "";
    		String cvsSplitBy = ";";
    		String[] row2 = null;
    		try {
                    br = new BufferedReader(new FileReader(path));
                    this.columnNames = new ArrayList<String>(); //lista dei nomi delle colonne
                    row2 = (br.readLine()).split(cvsSplitBy);   //legge dal file la prima riga (separata da ;)
                    for(int i=0; i<row2.length; i++){   //per ogni "cella" della prima riga
                      columnNames.add(row2[i]); //aggiunge i nomi delle colonne a columnNames
                    }
                    this.numberAttributes = row2.length;    //numero di attributi
                    this.tableName = "TEST";
                    //crea un arraylist partitions (sono gli attributi) fatto da hashmap contenenti 'key' oggetto e 'value' arraylist, di taglia numberAttributes
                    ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(this.numberAttributes);
                    //per ogni attributo, crea l'hashmap da inserire in partitions
                    for (int i = 0; i < this.numberAttributes; i++) {
                         Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<Object, LongBigArrayBigList>();
                         partitions.add(partition); //add hashmap in arraylist
                     }
                    long tupleId = 0;
                    while ((line = br.readLine()) != null) {    //legge una riga per volta
                        row2 = line.split(cvsSplitBy);
                        List<String> row = new ArrayList<String>(); //arraylist che contiene i valori della tupla, 1 per ogni tupla.
                        for(int i=0; i<row2.length; i++){   //per ogni valore della tupla letta
                            row.add(row2[i]);   //aggiungi ad arraylist row i singoli valori letti (valori degli attributi)
                        }
                        // System.out.println("num attr: "+this.numberAttributes);
                        for (int i = 0; i < this.numberAttributes; i++) {   //per ogni attributo
                            Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = partitions.get(i); //prende la partition per l'attributo considerato
                            //System.out.println("get i: "+row.get(i));
                            String entry = row.get(i);  //prende il valore attributo della riga i-esima
                            if (partition.containsKey(entry)) { //se la partition (attributo considerato) contiene già il valore i-esimo
                                partition.get(entry).add(tupleId);  //prende tramite il valore la classe di equivalenza e aggiunge idTupla
                            } else {    //se non è contenuto il valore i-esimo per l'attributo considerato
                                //System.out.println("Creo una nuova part: "+entry+" per columns name "+columnNames.get(i));
                                LongBigArrayBigList newEqClass = new LongBigArrayBigList(); //arraylist con le classi di equivalenza
                                newEqClass.add(tupleId);    //aggiunge idTupla alla classe di equivalenza (arrayList)
                                partition.put(entry, newEqClass);   //aggiunge a partition coppia "valore,EQclass"
                            }
                            //System.out.println("PARTIZIONI PER ATTRIBUTO"+partition.values());
                        }
                        tupleId++;  //passa alla seconda tupla
                    }//end while
                    this.numberTuples = tupleId;
                    return partitions;  //ritorna le partitions settate da csv
                }catch (FileNotFoundException e) {
                    e.printStackTrace();
                }catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) {
                        try {
                            br.close();
                        }catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            //System.out.println("Done");
            }
        //System.out.println("2");
        return new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(0);
    }
    
    private ArrayList<HashMap<Long, String>> createTable(int indexA, int indexB){
        ArrayList<HashMap<Long, String>> A_B = new ArrayList<HashMap<Long, String>>();
        //Per A
        HashMap<Long, String> tabella_A = new HashMap<Long, String>();
        Object2ObjectOpenHashMap<Object, LongBigArrayBigList> A = partitions.get(indexA);
        for(Object val : A.keySet()){
            LongBigArrayBigList EQ_idTuple = A.get(val);//prendo insieme di idTuple per questo 'val'
            for(long id : EQ_idTuple){
                tabella_A.put(id, (String) val);
            }
        }
        /*for(long t : tabella_A.keySet()){
            System.out.println("id : " + t + ", val : " + tabella_A.get(t) + "\n");
        }*/
        A_B.add(tabella_A);
        //Per B
        HashMap<Long, String> tabella_B = new HashMap<Long, String>();
        Object2ObjectOpenHashMap<Object, LongBigArrayBigList> B = partitions.get(indexB);
        for(Object val : B.keySet()){
            LongBigArrayBigList EQ_idTuple = B.get(val);//prendo insieme di idTuple per questo 'val'
            for(long id : EQ_idTuple){
                tabella_B.put(id, (String) val);
            }
        }
        A_B.add(tabella_B);
        
        return A_B;
    }

    /**
     * Initialize Cplus (resp. rhsCandidates) for each combination of the level.
     */
    private void initializeCplusForLevel() {
        for (OpenBitSet X : level1.keySet()) {
            ObjectArrayList<OpenBitSet> CxwithoutA_list = new ObjectArrayList<OpenBitSet>();
            // clone of X for usage in the following loop
            OpenBitSet Xclone = (OpenBitSet) X.clone();
            for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {
                Xclone.clear(A);    //setta a 0 il bit in posizione A del XCLONE
                OpenBitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                CxwithoutA_list.add(CxwithoutA);
                Xclone.set(A);
            }

            OpenBitSet CforX = new OpenBitSet();

            if (!CxwithoutA_list.isEmpty()) {
                CforX.set(1, numberAttributes + 1);
                for (OpenBitSet CxwithoutA : CxwithoutA_list) {
                    CforX.and(CxwithoutA);  //intersezione
                }
            }

            CombinationHelper ch = level1.get(X);
            ch.setRhsCandidates(CforX); //setta i Cc+ di X = CforX che contiene intersezione.
        }
    }

    /**
     * Computes the dependencies for the current level (level1).
     *
     * @throws AlgorithmExecutionException
     */
    private void computeODs(int l) throws AlgorithmExecutionException {
        
        initializeCplusForLevel();  //viene eseguito Cc+(X) = intersezione.....
        // iterate through the combinations of the level
forall: for (OpenBitSet X : level1.keySet()) {  //for all X € Ll
            if (level1.get(X).isValid()) {
                //riga 3 da qui...
                if(l==2){
                    for(int i=0; i<numberAttributes-1; i++){
                        for(int j=i+1; j<numberAttributes; j++){                    
                            OpenBitSet AB = new OpenBitSet();
                            AB.set(i+1);
                            AB.set(j+1);
                            //System.out.println("" + columnIdentifiers.get(i));
                            //System.out.println("" + columnIdentifiers.get(j));
                            //System.out.println("");
                            level1.get(AB).setRhsCandidatesS(AB);
                        }
                    }
                    break forall;
                }
                else if(l>2){
                        ObjectArrayList<OpenBitSet> CsXwithoutC_list = new ObjectArrayList<OpenBitSet>();
                        // clone of X for usage in the following loop
                        OpenBitSet Xclone = (OpenBitSet) X.clone();
                        for (int C = X.nextSetBit(0); C >= 0; C = X.nextSetBit(C + 1)) {
                            Xclone.clear(C);    //setta a 0 il bit di C del XCLONE
                            OpenBitSet CsXwithoutC = level0.get(Xclone).getRhsCandidatesS(); //get Cs+ di X senza C
                            CsXwithoutC_list.add(CsXwithoutC);
                            Xclone.set(C);  //setto C nel clone
                        }//da qui ho Cs+ di X\C
                        OpenBitSet U = new OpenBitSet();
                        if (!CsXwithoutC_list.isEmpty()) {
                            //U.set(1, numberAttributes + 1);
                            for (OpenBitSet CsXwithoutC : CsXwithoutC_list) {
                                U.or(CsXwithoutC); //unione
                            }
                        }//qui ho l'unione "U" dei Cs+ di X\C per ogni C di X.

                        //calcolo coppie di AB (A!=B) come bitset
                        ArrayList<int[]> AB_list = new ArrayList<int[]>();
                        for(int A = U.nextSetBit(0); A >= 0; A = U.nextSetBit(A+1)){
                            for(int B = U.nextSetBit(A); B >= 0; B = U.nextSetBit(B+1)){
                                if(A!=B){
                                    int[] AB = new int[2];
                                    AB[0] = A;
                                    AB[1] = B;
                                    AB_list.add(AB);
                                }
                            }
                        }//coppie AB di U calcolate

                        //calcolo di X\AB (insieme di attributi D)
                        OpenBitSet XwithoutAB = (OpenBitSet) X.clone();
                        for(int[] AB : AB_list){   //AB_list contiene solo bitset con 2 bit
                            int A = AB[0];
                            int B = AB[1];
                            XwithoutAB.clear(A);
                            XwithoutAB.clear(B);
                            //calcolo Cs+ di X\D
                            OpenBitSet Xclone2 = (OpenBitSet) X.clone();
                            for(int D = XwithoutAB.nextSetBit(0); D >= 0; D = XwithoutAB.nextSetBit(D + 1)) {   //per ogni D in X\AB
                                Xclone2.clear(D);   //setta a 0 il bit di D
                                OpenBitSet CsXwithoutD = level0.get(Xclone2).getRhsCandidatesS(); //Cs+ di X senza D (D è un attributo preso dal set X\AB)
                                //verifico per ogni coppia A,B di attributi di U se tale coppia APPARTIENE a Cs+ di X\D
                                if((CsXwithoutD.getBit(AB[0])==1) && (CsXwithoutD.getBit(AB[1])==1)){
                                    //la coppia A,B di U appartiene a CsX di X\D quindi viene settata
                                    level1.get(X).getRhsCandidatesS().set(AB[0]);
                                    level1.get(X).getRhsCandidatesS().set(AB[1]);
                                }
                                Xclone2.set(D);  //ri-setto D nel clone
                            }
                            XwithoutAB.set(A);
                            XwithoutAB.set(B);
                        }
                    }
            }
        }//end for
            
        // iterate through the combinations of the level
        for (OpenBitSet X : level1.keySet()) {  //for all X € Ll
            if (level1.get(X).isValid()) {
                
                // Build the intersection between X and C_plus(X) !!!
                OpenBitSet C_plus = level1.get(X).getRhsCandidates();
                OpenBitSet intersection = (OpenBitSet) X.clone();
                intersection.intersect(C_plus); //C+ di X creato
            
                // clone of X for usage in the following loop
                OpenBitSet Xclone = (OpenBitSet) X.clone();
                
                // iterate through all elements (A) of the intersection
                for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {
                    Xclone.clear(A);

                    // check if X\A -> A is valid
                    StrippedPartition spXwithoutA = level0.get(Xclone).getPartition();
                    StrippedPartition spX = level1.get(X).getPartition();
                    
                    if(spX.getError() == spXwithoutA.getError()){
                        // found Dependency
                        OpenBitSet XwithoutA = (OpenBitSet) Xclone.clone();
                        //X\A -> A
                        //for all B€X, A€X, A€Cc+(X\B)
                        //creo OrderDependency
                        
                        OrderDependency newOD = new OrderDependency(false, XwithoutA, A);
                        addOrderDependencyToResultReceiver(newOD);
                        numFD++;

                        // remove A from C_plus(X)
                        level1.get(X).getRhsCandidates().clear(A);

                        // remove all B in R\X from C_plus(X
                        OpenBitSet RwithoutX = new OpenBitSet();
                        // set to R
                        RwithoutX.set(1, numberAttributes + 1);
                        // remove X
                        RwithoutX.andNot(X);

                        for(int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                            level1.get(X).getRhsCandidates().clear(i);
                        }
                    }
                    Xclone.set(A);
                }//end for
                
                //calcolo le coppie A,B dell insieme Cs+(X)
                //servono i Cs+ per ogni attributo di X???
                OpenBitSet Cs_plusX = (OpenBitSet) level1.get(X).getRhsCandidatesS();
                //calcolo coppie di AB (A!=B) come bitset
                ArrayList<int[]> AB_list = new ArrayList<int[]>();
                for(int A = Cs_plusX.nextSetBit(0); A >= 0; A = Cs_plusX.nextSetBit(A+1)){
                    for(int B = Cs_plusX.nextSetBit(A); B >= 0; B = Cs_plusX.nextSetBit(B+1)){
                        if(A!=B){
                            int[] AB = new int[2];
                            AB[0] = A;
                            AB[1] = B;
                            AB_list.add(AB);
                        }
                    }
                }//coppie AB calcolate.
                
                //r17
                //for all A,B in Cs+(X)
                for(int[] AB : AB_list){   //AB_list contiene solo bitset con 2 bit

                    int A = AB[0];
                    int B = AB[1];
                    //calcolo X\B
                    OpenBitSet XwithoutB = (OpenBitSet) X.clone();
                    XwithoutB.clear(B);
                    OpenBitSet CxwithoutB = level0.get(XwithoutB).getRhsCandidates();
                    //calcolo X\A
                    OpenBitSet XwithoutA = (OpenBitSet) X.clone();
                    XwithoutA.clear(A);
                    OpenBitSet CxwithoutA = level0.get(XwithoutA).getRhsCandidates();
                    
                    //se A non appartiene a C+(X\B) OR B non appartiene a C+(X\A)
                    if(CxwithoutB.getBit(A)!=1 || CxwithoutA.getBit(B)!=1){
                        if(level1.get(X).getRhsCandidatesS().getBit(A)==1)
                            level1.get(X).getRhsCandidatesS().clear(A);
                        if(level1.get(X).getRhsCandidatesS().getBit(B)==1)
                            level1.get(X).getRhsCandidatesS().clear(B);                        
                    }
                    else{   //controllo order compatibility (per ogni coppia A,B)
                        //System.out.println("entro qui");
                        boolean isValid = true;
                        OpenBitSet XwithoutAB = (OpenBitSet) X.clone();
                        XwithoutAB.clear(A);
                        XwithoutAB.clear(B);
                        //adesso dispongo di X/AB, è il contesto
                        //iterare sui singoli attributi di X/AB per ricavare il contesto
             forcx:    for (int C_X = XwithoutAB.nextSetBit(0); C_X >= 0; C_X = XwithoutAB.nextSetBit(C_X + 1)){   //il contesto deve essere di 1 solo attributo per ogni attr di X\AB
                            if(!isValid)
                                break forcx;
                            //ricreo associazione <idTupla,valore> per A e B
                            //A_B ha in posizione 0 la table per A e in pos 1 la table per B
                            ArrayList<HashMap<Long, String>> A_B = createTable(A-1, B-1);
                            //per ogni attributo di X/AB e per ogni classe di equivalenza dell'attributo
                            StrippedPartition EQ_attribute = new StrippedPartition(partitions.get(C_X - 1));
                            //in EQ_attribute ora ci sono le classi di equivalenza di C_X
                            for(LongBigArrayBigList EQ : EQ_attribute.getStrippedPartition()){
                                if(!isValid)
                                    break forcx;
                                //stiamo considerando ogni Classi di Equivalenza per C_X
                                //confrontiamo ogni coppia di tuple
                                for(long i=0; i<EQ.size64()-1; i++){
                                    if(!isValid)
                                        break forcx;
                                    for(long j=i+1; j<EQ.size64(); j++){
                                        if(!isValid)
                                            break forcx;
                                        //analizzo coppia di tuple di A
                                        String A_val1 = A_B.get(0).get(i+1);
                                        String A_val2 = A_B.get(0).get(j+1);
                                        if(A_val1.equalsIgnoreCase("?"))
                                            break forcx;
                                        if(A_val2.equalsIgnoreCase("?"))
                                            break forcx;
                                        if(isNumeric(A_val1) && isNumeric(A_val2)){
                                            if(!(Double.parseDouble(A_val1) <= Double.parseDouble(A_val2)))
                                                isValid = false;
                                        }
                                        else{   //isString
                                            if(A_val1.compareTo(A_val2) > 0)    //if val1 > val2 cmp>0
                                                isValid = false;
                                        }
                                        
                                        //analizzo coppia di tuple di B
                                        String B_val1 = A_B.get(1).get(i+1);
                                        String B_val2 = A_B.get(1).get(j+1);
                                        if(B_val1.equalsIgnoreCase("?"))
                                            break forcx;
                                        if(B_val2.equalsIgnoreCase("?"))
                                            break forcx;
                                        //se B è numerico
                                        if(isNumeric(B_val1) && isNumeric(B_val2)){
                                            if(!(Double.parseDouble(B_val1) <= Double.parseDouble(B_val2)))
                                                isValid = false;
                                        }
                                        else{   //isString
                                            if(B_val1.compareTo(B_val2) > 0)    //if val1 > val2 cmp>0
                                                isValid = false;
                                        }
                                    }
                                }
                            }
                            if(isValid){
                                OpenBitSet contesto = new OpenBitSet();
                                contesto.set(C_X);
                                OrderDependency newOD = new OrderDependency(true, contesto, A, B);
                                addOrderDependencyToResultReceiver(newOD);
                                numOCD++;
                                //remove A,B from CsX+
                                level1.get(X).getRhsCandidatesS().clear(A);
                                level1.get(X).getRhsCandidatesS().clear(B);
                            }
                        }
                    }
                    //System.gc();    //cancello cloni di X
                }// end for all {A,B}
            }
        }
    }

    public static boolean isNumeric(String strNum) {
        try {
            double d = Double.parseDouble(strNum);
        } catch (NumberFormatException | NullPointerException nfe) {
            return false;
        }
        return true;
    }
    
    /**
     * Prune the current level (level1) by removing all elements with no rhs candidates.
     * All keys are marked as invalid.
     * In case a key is found, minimal dependencies are added to the result receiver.
     *
     * @throws AlgorithmExecutionException if the result receiver cannot handle the functional dependency.
     */
    private void pruneLevels(int l) throws AlgorithmExecutionException {
        ObjectArrayList<OpenBitSet> elementsToRemove = new ObjectArrayList<OpenBitSet>();
        for (OpenBitSet x : level1.keySet()) {
            if(l>=2){
                if(level1.get(x).getRhsCandidates().isEmpty() && level1.get(x).getRhsCandidatesS().isEmpty()) {
                    elementsToRemove.add(x);
                    continue;
                }
            }
        }
        for (OpenBitSet x : elementsToRemove) {
            level1.remove(x);
        }
    }

    /**
     * Adds the FD lhs -> a to the resultReceiver and also prints the dependency.
     *
     * @param lhs: left-hand-side of the functional dependency
     * @param a:   dependent attribute. Possible values: 1 <= a <= maxAttributeNumber.
     * @throws CouldNotReceiveResultException if the result receiver cannot handle the functional dependency.
     */
    private void processFunctionalDependency(OpenBitSet lhs, int a)
            throws CouldNotReceiveResultException {
        addDependencyToResultReceiver2(lhs, a);
    }

    /**
     * Calculate the product of two stripped partitions and return the result as a new stripped partition.
     *
     * @param pt1: First StrippedPartition
     * @param pt2: Second StrippedPartition
     * @return A new StrippedPartition as the product of the two given StrippedPartitions.
     */
    public StrippedPartition multiply(StrippedPartition pt1, StrippedPartition pt2) {
        ObjectBigArrayBigList<LongBigArrayBigList> result = new ObjectBigArrayBigList<LongBigArrayBigList>();
        ObjectBigArrayBigList<LongBigArrayBigList> pt1List = pt1.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> pt2List = pt2.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> partition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        long noOfElements = 0;
        // iterate over first stripped partition and fill tTable.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, i);
            }
            partition.add(new LongBigArrayBigList());
        }
        // iterate over second stripped partition.
        for (long i = 0; i < pt2List.size64(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable.get(t_id) != -1) {
                    partition.get(tTable.get(t_id)).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable.get(tId) != -1) {
                    if (partition.get(tTable.get(tId)).size64() > 1) {
                        LongBigArrayBigList eqClass = partition.get(tTable.get(tId));
                        result.add(eqClass);
                        noOfElements += eqClass.size64();
                    }
                    partition.set(tTable.get(tId), new LongBigArrayBigList());
                }
            }
        }
        // cleanup tTable to reuse it in the next multiplication.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, -1);
            }
        }
        return new StrippedPartition(result, noOfElements);
    }

    private long getLastSetBitIndex(OpenBitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
    }

    /**
     * Get prefix of OpenBitSet by copying it and removing the last Bit.
     *
     * @param bitset
     * @return A new OpenBitSet, where the last set Bit is cleared.
     */
    private OpenBitSet getPrefix(OpenBitSet bitset) {
        OpenBitSet prefix = (OpenBitSet) bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }

    /**
     * Build the prefix blocks for a level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes as  the value.
     */
    private void buildPrefixBlocks() {
        this.prefix_blocks.clear();
        for (OpenBitSet level_iter : level0.keySet()) {
            OpenBitSet prefix = getPrefix(level_iter);

            if (prefix_blocks.containsKey(prefix)) {
                prefix_blocks.get(prefix).add(level_iter);
            } else {
                ObjectArrayList<OpenBitSet> list = new ObjectArrayList<OpenBitSet>();
                list.add(level_iter);
                prefix_blocks.put(prefix, list);
            }
        }
    }

    /**
     * Get all combinations, which can be built out of the elements of a prefix block
     *
     * @param list: List of OpenBitSets, which are in the same prefix block.
     * @return All combinations of the OpenBitSets.
     */
    private ObjectArrayList<OpenBitSet[]> getListCombinations(ObjectArrayList<OpenBitSet> list) {
        ObjectArrayList<OpenBitSet[]> combinations = new ObjectArrayList<OpenBitSet[]>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                OpenBitSet[] combi = new OpenBitSet[2];
                combi[0] = list.get(a);
                combi[1] = list.get(b);
                combinations.add(combi);
            }
        }
        return combinations;
    }

    /**
     * Checks whether all subsets of X (with length of X - 1) are part of the last level.
     * Only if this check return true X is added to the new level.
     *
     * @param X
     * @return
     */
    private boolean checkSubsets(OpenBitSet X) {
        boolean xIsValid = true;

        // clone of X for usage in the following loop
        OpenBitSet Xclone = (OpenBitSet) X.clone();

        for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
            Xclone.clear(l);
            if (!level0.containsKey(Xclone)) {
                xIsValid = false;
                break;
            }
            Xclone.set(l);
        }

        return xIsValid;
    }

    private void calculateNextLevel(int l) {
        level0 = level1;
        level1 = null;
        System.gc();

        Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper> new_level = new Object2ObjectOpenHashMap<OpenBitSet, CombinationHelper>();

        buildPrefixBlocks();    //genera i blocchi che differiscono di un attributo

        for (ObjectArrayList<OpenBitSet> prefix_block_list : prefix_blocks.values()) {
            //continue only, if the prefix_block contains at least 2 elements
            if (prefix_block_list.size() < 2) {
                continue;
            }

            ObjectArrayList<OpenBitSet[]> combinations = getListCombinations(prefix_block_list);

            for (OpenBitSet[] c : combinations) {
                OpenBitSet X = (OpenBitSet) c[0].clone();   //clona blocco c di combinations
                //c[0] è il primo openbitset della combinazione
                X.or(c[1]); //controlla se il primo attributo è uguale al secondo attributo

                if (checkSubsets(X)){ //if X/A € Ll
                    StrippedPartition st = null;
                    CombinationHelper ch = new CombinationHelper();
                    if (level0.get(c[0]).isValid() && level0.get(c[1]).isValid()) {
                        st = multiply(level0.get(c[0]).getPartition(), level0.get(c[1]).getPartition());
                    } else {
                        ch.setInvalid();
                    }
                    OpenBitSet rhsCandidates = new OpenBitSet();
                    OpenBitSet rhsCandidates_S = new OpenBitSet();

                    ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);
                    ch.setRhsCandidatesS(rhsCandidates_S);

                    new_level.put(X, ch);
                }
            }
        }
        level1 = new_level;
    }


    /**
     * Add the functional dependency to the ResultReceiver.
     *
     * @param X: A OpenBitSet representing the Columns of the determinant.
     * @param a: The number of the dependent column (starting from 1).
     * @throws CouldNotReceiveResultException if the result receiver cannot handle the functional dependency.
     */
    private void addDependencyToResultReceiver(OpenBitSet X, int a) throws CouldNotReceiveResultException {
        //if (this.fdResultReceiver == null) {
        //    System.out.println("Sono entrata");
        //	return;
        //}
        ColumnIdentifier[] columns = new ColumnIdentifier[(int) X.cardinality()];
        int j = 0;
        for (int i = X.nextSetBit(0); i >= 0; i = X.nextSetBit(i + 1)) {
            columns[j++] = this.columnIdentifiers.get(i - 1);
            System.out.print(columnIdentifiers.get(i - 1)+" ");
        }
        //ColumnCombination colCombination = new ColumnCombination(columns);
        //FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get((int) a - 1));
        System.out.print(" -> ");
        System.out.println(columnIdentifiers.get((int) a - 1));
        //this.fdResultReceiver.receiveResult(fdResult);
    }
    
    private void addDependencyToResultReceiver2(OpenBitSet X, int a) throws CouldNotReceiveResultException {
        //if (this.fdResultReceiver == null) {
        //    System.out.println("Sono entrata");
        //	return;
        //}
        ColumnIdentifier[] columns = new ColumnIdentifier[(int) X.cardinality()];
        int j = 0;
        System.out.print(columnIdentifiers.get((int) a - 1));
        System.out.print("_0,");
        for (int i = X.nextSetBit(0); i >= 0; i = X.nextSetBit(i + 1)) {
            columns[j++] = this.columnIdentifiers.get(i - 1);
            System.out.print(columnIdentifiers.get(i - 1)+"_0,");
        }
        System.out.println();
        //ColumnCombination colCombination = new ColumnCombination(columns);
        //FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get((int) a - 1));
        //System.out.print(" -> ");
        //System.out.println(columnIdentifiers.get((int) a - 1));
        //this.fdResultReceiver.receiveResult(fdResult);
    }

    /**
     * Add the functional dependency to the ResultReceiver.
     *
     * @param X: A OpenBitSet representing the Columns of the determinant.
     * @param a: The number of the dependent column (starting from 1).
     * @throws CouldNotReceiveResultException if the result receiver cannot handle the functional dependency.
     */
    private void addOrderDependencyToResultReceiver(OrderDependency newOD) throws CouldNotReceiveResultException {
        if(newOD.isType()){ //canonica
            OpenBitSet X = newOD.getContext();
            for (int i = X.nextSetBit(0); i >= 0; i = X.nextSetBit(i + 1)) {
                //System.out.print(columnIdentifiers.get(i - 1)+" ");
                out.print(columnIdentifiers.get(i - 1)+" ");
            }
            /*
            System.out.print(" : ");
            System.out.print(columnIdentifiers.get((int) newOD.getA() - 1));
            System.out.print(" ~ ");
            System.out.println(columnIdentifiers.get((int) newOD.getB() - 1));
            */
            //stampa su file output
            out.print(" : ");
            out.print(columnIdentifiers.get((int) newOD.getA() - 1));
            out.print(" ~ ");
            out.println(columnIdentifiers.get((int) newOD.getB() - 1));
            
        }
        else{
            OpenBitSet X = newOD.getContext();
            for (int i = X.nextSetBit(0); i >= 0; i = X.nextSetBit(i + 1)) {
                //System.out.print(columnIdentifiers.get(i - 1)+" ");
                //stampa su file output
                out.print(columnIdentifiers.get(i - 1)+" ");
            }
            /*
            System.out.print(" -> ");
            System.out.println(columnIdentifiers.get((int) newOD.getA() - 1));
            */
            //stampa su file output
            out.print(" -> ");
            out.println(columnIdentifiers.get((int) newOD.getA() - 1));
        }
    }
    
    private void setColumnIdentifiers() {
        this.columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(this.columnNames.size());
        for (String column_name : this.columnNames) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, column_name));
        }
    }

    public void serialize_attribute(OpenBitSet bitset, CombinationHelper ch) {
        String file_name = bitset.toString();
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(file_name));
            oos.writeObject(ch);
            oos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CombinationHelper deserialize_attribute(OpenBitSet bitset) {
        String file_name = bitset.toString();
        ObjectInputStream is = null;
        CombinationHelper ch = null;
        try {
            is = new ObjectInputStream(new FileInputStream(file_name));
            ch = (CombinationHelper) is.readObject();
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return ch;
    }
    
    private void stampaTempo(){
    	//System.out.print((System.currentTimeMillis() - tempoStart)/1000 + "." + (System.currentTimeMillis()- tempoStart)%1000+"  ");
        //stampa su file log
        log.print((System.currentTimeMillis() - tempoStart)/1000 + "." + (System.currentTimeMillis()- tempoStart)%1000+"  ");
    }
}
