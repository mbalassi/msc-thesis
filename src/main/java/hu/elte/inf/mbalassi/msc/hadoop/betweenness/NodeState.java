package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

/**
 * State of a node in one BFS calculation
 * 
 */
enum NodeState {
    INACTIVE((byte)0), // Inactive
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
    public static NodeState fromByte(byte b) {
        return byteToState[b];
    }
    
    /** serialized form */
    private final byte b;
    
    /**
     * Create a {@link NodeState} instance with the given byte representation.
     * 
     * @param b byte representation of the state.
     */
    NodeState(byte b) {
        this.b = b;
    }
    
    /** The ith state is represented by the byte i */
    private static final NodeState[] byteToState;
    static {
        byteToState = new NodeState[8];
        for (NodeState state : NodeState.values()) {
            byteToState[state.toByte()] = state;
        }
    }
}
