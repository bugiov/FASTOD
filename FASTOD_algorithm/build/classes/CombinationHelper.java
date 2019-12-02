import org.apache.lucene.util.OpenBitSet;

import java.io.Serializable;

public class CombinationHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    private OpenBitSet rhsCandidates;
    private OpenBitSet rhsCandidatesS;
    private boolean valid;

    private StrippedPartition partition;

    public CombinationHelper() {
        valid = true;
    }

    public OpenBitSet getRhsCandidates() {
        return rhsCandidates;
    }

    public void setRhsCandidates(OpenBitSet rhsCandidates) {
        this.rhsCandidates = rhsCandidates;
    }

    public OpenBitSet getRhsCandidatesS() {
        return rhsCandidatesS;
    }

    public void setRhsCandidatesS(OpenBitSet rhsCandidatesS) {
        this.rhsCandidatesS = rhsCandidatesS;
    }
    
    public StrippedPartition getPartition() {
        return partition;
    }

    public void setPartition(StrippedPartition partition) {
        this.partition = partition;
    }

    public boolean isValid() {
        return valid;
    }

    public void setInvalid() {
        this.valid = false;
        partition = null;
    }

}
