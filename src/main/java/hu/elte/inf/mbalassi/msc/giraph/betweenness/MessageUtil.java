package hu.elte.inf.mbalassi.msc.giraph.betweenness;

import java.nio.ByteBuffer;

/**
 * Utility functions for reading and writing messages.
 * 
 */
class MessageUtil {
    
    /**
     * Value class for storing the content of SCORE messages.
     */
    static class Score {
        public int bfsId;
        public double pathsTo;
        public double score;
    }
    
    /**
     * Value class for storing the content of DISTANCE messages.
     */
    static class Distance {
        public int parId;
        public int bfsId;
        public double paths;
    }
    
    /**
     * Value class for storing the content of ACKNOWLEDGE_ROUTE messages.
     */
    static class Ack {
        public int bfsId;
    }
    
    /**
     * Returns the type of the given message.
     * 
     * @param array the message.
     * @return the type of the message.
     */
    static MessageType typeOf(byte[] array) {
        return MessageType.fromByte(array[0]);
    }
    
    /**
     * Serializes a distance into a message.
     * 
     * @param d the distance.
     * @return the message.
     */
    static byte[] fromDist(Distance dist) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + 8);
        buf.put(MessageType.DISTANCE.toByte());
        buf.putInt(dist.parId);
        buf.putInt(dist.bfsId);
        buf.putDouble(dist.paths);
        return buf.array();
    }
    
    /**
     * Deserializes a distance message.
     * 
     * @param array the message.
     * @return the distance.
     */
    static Distance toDist(byte[] array) {
        ByteBuffer buf = ByteBuffer.wrap(array);
        if (buf.get() != MessageType.DISTANCE.toByte()) {
            throw new IllegalArgumentException("Could not deserialize: message is not of type DISTANCE");
        }
        Distance dist = new Distance();
        dist.parId = buf.getInt();
        dist.bfsId = buf.getInt();
        dist.paths = buf.getDouble();
        return dist;
    }
    
    /**
     * Serializes an acknowledgment into a message.
     * 
     * @param ack the acknowledgment.
     * @return the message.
     */
    static byte[] fromAck(Ack ack) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4);
        buf.put(MessageType.ACKNOWLEDGE_ROUTE.toByte());
        buf.putInt(ack.bfsId);
        return buf.array();
    }
    
    /**
     * Deserializes an acknowledgment message.
     * 
     * @param array the message.
     * @return the acknowledgment.
     */
    static Ack toAck(byte[] array) {
        ByteBuffer buf = ByteBuffer.wrap(array);
        if (buf.get() != MessageType.ACKNOWLEDGE_ROUTE.toByte()) {
            throw new IllegalArgumentException("Could not deserialize: message is not of type ACKNOWLEDGE_ROUTE");
        }
        Ack ack = new Ack();
        ack.bfsId = buf.getInt();
        return ack;
    }
    
    /**
     * Serializes a pathsTo-score pair into a message.
     * 
     * @param pathsTo paths to the sending node.
     * @param score the score of the sending node.
     * @return the message.
     */
    static byte[] fromScore(Score score) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 8 + 8);
        buf.put(MessageType.SCORE.toByte());
        buf.putInt(score.bfsId);
        buf.putDouble(score.pathsTo);
        buf.putDouble(score.score);
        return buf.array();
    }
    
    /**
     * Deserializes a distance message.
     * 
     * @param array the message.
     * @return the distance.
     */
    static Score toScore(byte[] array) {
        ByteBuffer buf = ByteBuffer.wrap(array);
        if (buf.get() != MessageType.SCORE.toByte()) {
            throw new IllegalArgumentException("Could not deserialize: message is not of type SCORE");
        }
        
        Score res = new Score();
        res.bfsId = buf.getInt();
        res.pathsTo = buf.getDouble();
        res.score = buf.getDouble();
        return res;
    } 

}
