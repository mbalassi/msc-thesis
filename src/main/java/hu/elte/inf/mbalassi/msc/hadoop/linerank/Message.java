package hu.elte.inf.mbalassi.msc.hadoop.linerank;

import java.nio.ByteBuffer;

/**
 * Internal representation for edges, nodes and messages.
 * 
 */
public class Message {
    
    /**
     * Data stored at a node.
     */
    public static class NodeData {
        // score during the last iteration
        double lastscore;
        // probability of a restart starting at this node
        double startAtNodeProb;
        // outgoing edges
        int[] edges;
        
        public NodeData() {}
        
        /**
         * Number of bytes needed to store the node data.
         * 
         * @return the size of the data in bytes
         */
        public int sizeInBytes() {
            return 8 + 8 + 4 + 4 * edges.length;
        }
        
        /**
         * Write node data to the given byte buffer.
         * 
         * @param buf the given byte buffer
         */
        public void writeTo(ByteBuffer buf) {
            buf.putDouble(lastscore);
            buf.putDouble(startAtNodeProb);
            buf.putInt(edges.length);
            for (int i : edges) {
                buf.putInt(i);
            }
        }
        
        /**
         * Read node data from the given byte buffer.
         * 
         * @param buf the given byte buffer
         */
        public NodeData(ByteBuffer buf) {
            lastscore = buf.getDouble();
            startAtNodeProb = buf.getDouble();
            edges = new int[buf.getInt()];
            for (int i = 0; i < edges.length; ++i) {
                edges[i] = buf.getInt();
            }
        }
    }
    
    // current score of the node
    double score;
    // node data
    NodeData data;
    
    /**
     * Convert the {@link Message} to a byte array.
     * 
     * @return the given message as a byte array
     */
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(8 + 1 + (data == null ? 0 : data.sizeInBytes()));
        buf.putDouble(score);
        if (data == null) {
            buf.put((byte)0);
        } else {
            buf.put((byte)1);
            data.writeTo(buf);
        }
        return buf.array();
    }
    
    /**
     * Create an empty message.
     */
    public Message() {}
    
    /**
     * Parse the message from the given byte array.
     * 
     * @param bytes the byte array containing a message.
     */
    public Message(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        score = buf.getDouble();
        if (buf.get() == (byte)1) {
            data = new NodeData(buf);
        }
    }
    
    /**
     * Combine two messages in this {@link Message} object.
     * 
     * @param other the message to combine with
     */
    public void merge(Message other) {
        if (other.data != null) {
            if (data != null) {
                throw new IllegalArgumentException("Only one message can contain node data");
            } else {
                data = other.data;
            }
        }
        score += other.score;
    }
}
