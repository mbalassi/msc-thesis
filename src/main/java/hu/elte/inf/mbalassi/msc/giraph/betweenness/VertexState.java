package hu.elte.inf.mbalassi.msc.giraph.betweenness;

/**
 * State of a node in one BFS calculation
 * 
 */
enum VertexState {
    INACTIVE((byte)0), // uninitialized
    SENDING_DIST_MESSAGES((byte)1), // received its distance
    WAIT_FOR_CHILDREN_ACK((byte)2), // waiting for children to acknowledge being added to the tree
    WAIT_FOR_CHILDREN_SCORE((byte)3), // waiting for the score of its child nodes
    SENDING_SCORE_MESSAGES((byte)4), // score calculated
    FINISHED((byte)5); // score sent to parents
    
    /**
     * Returns the byte representation of the state.
     * 
     * @return the byte representation.
     */
    public byte toByte() {
        return b;
    }
    
    /**
     * Returns the state corresponding to the given byte.
     * 
     * @param b the given byte.
     * @return the state represented by the byte.
     */
    public static VertexState fromByte(byte b) {
        return byteToState[b];
    }
    
    /** serialized form */
    private final byte b;
    
    /**
     * Create a {@link VertexState} instance with the given byte representation.
     * 
     * @param b byte representation of the state.
     */
    VertexState(byte b) {
        this.b = b;
    }
    
    /** The ith state is represented by the byte i */
    private static final VertexState[] byteToState;
    static {
        byteToState = new VertexState[8];
        for (VertexState state : VertexState.values()) {
            byteToState[state.toByte()] = state;
        }
    }
}
