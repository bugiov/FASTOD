
import org.apache.lucene.util.OpenBitSet;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author maico
 */
public class OrderDependency {
    private boolean type;   //standard o canonica?
    private OpenBitSet context;
    private int A;
    private int B;   //null per le standard

    /*
    * Costruttore per le OD canoniche (OCD)
    * type = true if OD is Canonical
    */
    public OrderDependency(boolean type, OpenBitSet context, int A, int B) {
        this.type = type;
        this.context = context;
        this.A = A;
        this.B = B;
    }
    
    /*
    * Costruttore per le OD standard
    */
    public OrderDependency(boolean type, OpenBitSet context, int A) {
        this.type = type;
        this.context = context;
        this.A = A;
        this.B = 0;
    }

    public boolean isType() {
        return type;
    }

    public void setType(boolean type) {
        this.type = type;
    }

    public OpenBitSet getContext() {
        return context;
    }

    public void setContext(OpenBitSet context) {
        this.context = context;
    }

    public int getA() {
        return A;
    }

    public void setA(int A) {
        this.A = A;
    }

    public int getB() {
        return B;
    }

    public void setB(int B) {
        this.B = B;
    }
    
    
}
