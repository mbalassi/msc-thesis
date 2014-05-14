package hu.elte.inf.mbalassi.msc.hadoop.scc;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;

/**
 * Utility functions for reading and writing messages.
 * 
 */
class MessageUtil {
    
    /**
     * Internal value class to store information in intermediate messages.
     */
    static class Message {
        public final MessageType type;
        public final int label;
        public final int[] edges;
        
        public Message(int label) {
            this.type = MessageType.LABEL;
            this.label = label;
            this.edges = null;
        }
        
        public Message(boolean isActive, int label, int[] edges) {
            this.type = isActive ? MessageType.ACTIVE_NODE : MessageType.INACTIVE_NODE;
            this.label = label;
            this.edges = edges;
        }
    }
    
    /**
     * Serializes info into a message.
     * 
     * @param node the information.
     * @return the message.
     */
    public static BytesWritable fromMessage(Message msg) {
        int capacity = 5;
        if(msg.type != MessageType.LABEL) {
            capacity += 4 * msg.edges.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(capacity);
        buf.put(msg.type.toByte());
        buf.putInt(msg.label);
        if (msg.type != MessageType.LABEL) {
            buf.asIntBuffer().put(msg.edges);
        }
        return new BytesWritable(buf.array());
    }
    
    /**
     * Deserializes info from a message.
     * 
     * @param array the message.
     * @return the information.
     */
    public static Message toMessage(BytesWritable value) {
        int length = value.getLength();
        ByteBuffer buf = ByteBuffer.wrap(value.getBytes());
        int label;
        int[] edges;
        switch (MessageType.fromByte(buf.get())) {
            case LABEL:
                return new Message(buf.getInt());
            case ACTIVE_NODE:
                label = buf.getInt();
                edges = new int[(length-5) / 4];
                buf.asIntBuffer().get(edges);
                return new Message(true, label, edges);
            case INACTIVE_NODE:
                label = buf.getInt();
                edges = new int[(length-5) / 4];
                buf.asIntBuffer().get(edges);
                return new Message(false, label, edges);
            default:
                throw new AssertionError();
        }
    }
}
