package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

/**
 * Types of messages sent from mappers to reducers.
 * 
 */
enum MessageType {
    NODE((byte)0), // node data
    DISTANCE((byte)1), // distance to node
    ACKNOWLEDGE_ROUTE((byte)2), // child node acknowledging parent
    SCORE((byte)3); // sending number of paths to and score of child node
    
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
    public static MessageType fromByte(byte b) {
        return byteToType[b];
    }
    
    /** serialized form */
    private final byte b;
    
    /**
     * Create a {@link MessageType} instance with the given byte representation.
     * 
     * @param b byte representation of the state.
     */
    MessageType(byte b) {
        this.b = b;
    }
    
    /** The ith type is represented by the byte i */
    private static final MessageType[] byteToType;
    static {
        byteToType = new MessageType[4];
        for (MessageType type : MessageType.values()) {
            byteToType[type.toByte()] = type;
        }
    }
}
