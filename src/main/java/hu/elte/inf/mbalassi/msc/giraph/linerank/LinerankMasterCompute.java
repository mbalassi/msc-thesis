package hu.elte.inf.mbalassi.msc.giraph.linerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.master.MasterCompute;

/**
 * Master computation managing iterations for LineRank calculation.
 * 
 */
public class LinerankMasterCompute extends MasterCompute {
    
    public static final String MORE_ITERATIONS_AGG = "MORE_ITERATIONS";

    @Override
    public void readFields(DataInput arg0) throws IOException {}

    @Override
    public void write(DataOutput arg0) throws IOException {}

    @Override
    public void compute() {}

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerAggregator(MORE_ITERATIONS_AGG, BooleanOrAggregator.class);
    }

}
