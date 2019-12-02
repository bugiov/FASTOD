import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

public class StrippedPartition {
    private double error;
    private long elementCount;
    private ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition = null;

    /**
     * Create a StrippedPartition with only one equivalence class with the definied number of elements. <br/>
     * Tuple ids start with 0 to numberOfElements-1
     *
     * @param numberTuples
     */
    public StrippedPartition(long numberTuples) {
        this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = numberTuples;
        // StrippedPartition only contains partition with more than one elements.
        if (numberTuples > 1) {
            LongBigArrayBigList newEqClass = new LongBigArrayBigList();
            for (int i = 0; i < numberTuples; i++) {
                newEqClass.add(i);
            }
            this.strippedPartition.add(newEqClass);
        }
        this.calculateError();
    }

    /**
     * Create a StrippedPartition from a HashMap mapping the values to the tuple ids.
     *
     * @param partition
     */
    public StrippedPartition(Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition) {
        this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = 0;

        //System.out.print("Valori: \n");
        
        //create stripped partitions -> only use equivalence classes with size > 1.
        for (LongBigArrayBigList eqClass : partition.values()) {
            if (eqClass.size64() > 1) {
                strippedPartition.add(eqClass);
                //System.out.println("Classe "+eqClass+" taglia "+eqClass.size64());
                elementCount += eqClass.size64();
            }
            //System.out.println("Conteggio elementi: "+elementCount);
            //System.out.println();
        }
        this.calculateError();
    }

    public StrippedPartition(ObjectBigArrayBigList<LongBigArrayBigList> sp, long elementCount) {
        this.strippedPartition = sp;
        this.elementCount = elementCount;
        this.calculateError();

    }

    public double getError() {
        return error;
    }
    
    public double getElementCount() {
        return elementCount;
    }

    public ObjectBigArrayBigList<LongBigArrayBigList> getStrippedPartition() {
        return this.strippedPartition;
    }

    private void calculateError() {
        // calculating the error. Dividing by the number of entries
        // in the whole population is not necessary.
        this.error = this.elementCount - this.strippedPartition.size64();
        
        //System.out.println("size stripped: "+this.strippedPartition.size64());
    	//System.out.println("errore: "+this.error);
    	//System.out.println("conteggio elementi: "+this.elementCount);
    }
    
    public double calculateError2(StrippedPartition X, long numberTuples) {
        // calculating the error. Dividing by the number of entries
        // in the whole population is not necessary.
    	//System.out.println("PARTIZIONE XwA");
    	//printPartition2();
    	//System.out.println("PARTIZIONE X");
    	//X.printPartition2();
        double sum=0;
        double value=0;
        //boolean notVerified=true;
        long tuple_parziali=numberTuples;
        for (int i=0; i<strippedPartition.size64(); i++) {
    		if(strippedPartition.get(i)!=null) {
    			tuple_parziali = tuple_parziali-strippedPartition.get(i).size64();
    			//notVerified=true;
    			LongBigArrayBigList max = null;
    			for (int j=0; j<X.getStrippedPartition().size64(); j++) {
    				if(strippedPartition.get(i).containsAll(X.getStrippedPartition().get(j))&& strippedPartition.get(i).size64()>=X.getStrippedPartition().get(j).size64()){
    					if(max == null){
        					max = X.getStrippedPartition().get(j);
        					//System.out.println("PRIMO MAX: per i "+i+" e j "+j+" = "+max.size64());
    					}
    					value = X.getStrippedPartition().get(j).size64();
    					if(value > max.size64()){
        					max = X.getStrippedPartition().get(j);
        					//System.out.println("TAGLIA MAX: per i "+i+" e j "+j+" = "+max.size64());
    					//break;
    					}
    				}
    			}
    			if(max==null){
    				sum+=1;
    				//sum+=(strippedPartition.get(i).size64()- 1);
    				//System.out.println("Aggiungo alla somma 1");
    				//System.out.println("SONO ENTRATO IN NOT VERIFIED");
    			}
    			else{
    				//System.out.println("MAX CONFERMATO: "+max.size64());
    				//System.out.println("Aggiungo alla somma: "+(strippedPartition.get(i).size64()- max.size64()));
    				sum+=max.size64();
    			}
    				
    		}
    	}
        
        //System.out.println("TUPLE PARZIALI: "+tuple_parziali);
        //System.out.println("SOMMA TOTALE: "+sum);
        //X.printPartition2();
        sum+=tuple_parziali;
        double error = 1- sum/numberTuples;
        return error;
        
        //System.out.println("size stripped: "+this.strippedPartition.size64());
    	//System.out.println("errore: "+this.error);
    	//System.out.println("conteggio elementi: "+this.elementCount);
    }
    
    public double calculateError3(StrippedPartition X, long numberTuples) {
        // calculating the error. Dividing by the number of entries
        // in the whole population is not necessary.
    	LongBigArrayBigList tTable=new LongBigArrayBigList(numberTuples);
    	
    	tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(0);
        }
    	 	
    	//System.out.println("PARTIZIONE XwA");
    	//printPartition2();
    	//System.out.println("PARTIZIONE X");
    	//X.printPartition2();
        double errore=0;
        //double value=0;
        //boolean notVerified=true;
        long t_id=0;
        long cardinality=0;
        for (int i=0; i<X.getStrippedPartition().size64(); i++) {
    		//tuple_parziali = tuple_parziali-X.getStrippedPartition().get(i).size64();
    		//notVerified=true;
        	t_id = X.getStrippedPartition().get(i).get(0);
        	tTable.set(t_id, X.getStrippedPartition().get(i).size64());
        	//System.out.println("INSERISCO "+X.getStrippedPartition().get(i).size64()+" PER "+X.getStrippedPartition().get(i).getLong(0));
        }
    	for (int i=0; i<strippedPartition.size64(); i++) {
    		cardinality=1;
    		for (int j=0; j<strippedPartition.get(i).size64(); j++) {
    			//System.out.println("PRENDO la taglia di "+strippedPartition.get(i).get(j)+" ed "+tTable.get(strippedPartition.get(i).get(j)));
    			if(cardinality<tTable.get(strippedPartition.get(i).get(j))){
    				cardinality=tTable.get(strippedPartition.get(i).get(j));
    			}
    			
    		}
    		errore+=(strippedPartition.get(i).size64()-cardinality);
    		//System.out.println("IL NUMERO TUPLE DA ELIMINARE PER "+strippedPartition.get(i)+" "+(strippedPartition.get(i).size64()-cardinality));
    	}
    	for (int i=0; i<X.getStrippedPartition().size64(); i++) {
    		t_id = X.getStrippedPartition().get(i).get(0);
        	tTable.set(t_id, 0);
    	}
    			
        return errore/numberTuples;
        
        //System.out.println("size stripped: "+this.strippedPartition.size64());
    	//System.out.println("errore: "+this.error);
    	//System.out.println("conteggio elementi: "+this.elementCount);
    }
    
    public void printPartition2() {
    	System.out.println("Partizione: ");
    	for (int i=0; i<strippedPartition.size64(); i++) {
    		if(strippedPartition.get(i)!=null)
    			System.out.println(""+i+" => "+strippedPartition.get(i)+", ");
    			//System.out.println("associato a: "+containTuples.get(i));
    	}
    	System.out.println();
    }

    public void empty() {
        this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = 0;
        this.error = 0.0;
    }
}
