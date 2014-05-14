package hu.elte.inf.mbalassi.msc.giraph.scc;

/**
 * The possible states of the SCC computation.
 * 
 * @author Péter Englert <engi.peti@gmail.com>
 */
public enum ComputationPhase {
    LABEL_PROPAGATION(0),
    TRANSPOSE_GRAPH(5),
    INIT_REVERSE_LABEL_PROPAGATION(3),
    REVERSE_LABEL_PROPAGATION(1),
    RESET_GRAPH(2),
    INIT_LABEL_PROPAGATION(6),
    INIT_COUNT(4),
    COUNT(7);
    
    /**
     * Get the integer value representing the phase.
     * 
     * @return the int value of the phase.
     */
    public int toInt() {
        return i;
    }
    
    /**
     * Get the computation phase represented by the given integet.
     * 
     * @param i the given integer
     * @return the computation phase
     */
    public static ComputationPhase fromInt(int i) {
        return intToPhase[i];
    }
    
    /** serialized form */
    private final int i;
    
    ComputationPhase(int i) {
        this.i = i;
    }
    
    /** The ith type is represented by the int i */
    private static final ComputationPhase[] intToPhase;
    static {
        intToPhase = new ComputationPhase[8];
        for (ComputationPhase type : ComputationPhase.values()) {
            intToPhase[type.toInt()] = type;
        }
    }
}
