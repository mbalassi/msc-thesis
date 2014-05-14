package hu.elte.inf.mbalassi.msc.giraph.tricnt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;

public class TriangleCounterMasterCompute extends MasterCompute {

    static enum Counters {
        TRI_CNT
    }

    public static final String TRI_CNT_AGG = "TRI_CNT";

    @Override
    public void readFields(DataInput arg0) throws IOException {}

    @Override
    public void write(DataOutput arg0) throws IOException {}

    @Override
    public void compute() {
        if (this.getSuperstep() == 3){
            long triCnt = ((LongWritable)this.getAggregatedValue(TRI_CNT_AGG)).get();
            this.getContext().getCounter(Counters.TRI_CNT).increment(triCnt);
            this.haltComputation();
        }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerAggregator(TRI_CNT_AGG, LongSumAggregator.class);
    }

}
