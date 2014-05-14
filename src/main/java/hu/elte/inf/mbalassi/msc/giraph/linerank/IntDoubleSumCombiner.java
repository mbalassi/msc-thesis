package hu.elte.inf.mbalassi.msc.giraph.linerank;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Combiner which sums double values belonging to the same integer ID.
 * 
 */
public class IntDoubleSumCombiner extends MessageCombiner<IntWritable, DoubleWritable> {

    @Override
    public void combine(IntWritable is, DoubleWritable score1, DoubleWritable score2) {
        score1.set(score1.get() + score2.get());
    }

    @Override
    public DoubleWritable createInitialMessage() {
        return new DoubleWritable(0.0);
    }

}
