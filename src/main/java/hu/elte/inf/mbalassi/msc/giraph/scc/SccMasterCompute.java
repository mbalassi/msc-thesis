package hu.elte.inf.mbalassi.msc.giraph.scc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Master computation managing phase transitions and iterations for SCC calculation.
 * 
 * @author Péter Englert <engi.peti@gmail.com>
 */
public class SccMasterCompute extends MasterCompute {
    
    public static final String MORE_ITERATIONS_AGG = "MORE_ITERATIONS";
    public static final String PHASE_AGG = "COMPUTATION_PHASE";

    @Override
    public void readFields(DataInput arg0) throws IOException {}

    @Override
    public void write(DataOutput arg0) throws IOException {}

    @Override
    public void compute() {
        if (getSuperstep() < 2) {
            setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.LABEL_PROPAGATION.toInt()));
        } else {
            switch (ComputationPhase.fromInt(((IntWritable)getAggregatedValue(PHASE_AGG)).get())) {
                case LABEL_PROPAGATION:
                    if(!((BooleanWritable)getAggregatedValue(MORE_ITERATIONS_AGG)).get()) {
                        setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.TRANSPOSE_GRAPH.toInt()));
                    }
                    break;
                case TRANSPOSE_GRAPH:
                    setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.INIT_REVERSE_LABEL_PROPAGATION.toInt()));
                    break;
                case INIT_REVERSE_LABEL_PROPAGATION:
                    setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.REVERSE_LABEL_PROPAGATION.toInt()));
                    break;
                case REVERSE_LABEL_PROPAGATION:
                    if(!((BooleanWritable)getAggregatedValue(MORE_ITERATIONS_AGG)).get()) {
                        setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.RESET_GRAPH.toInt()));
                    }
                    break;
                case RESET_GRAPH:
                    setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.INIT_LABEL_PROPAGATION.toInt()));
                    break;
                case INIT_LABEL_PROPAGATION:
                    if(((BooleanWritable)getAggregatedValue(MORE_ITERATIONS_AGG)).get()) {
                        setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.LABEL_PROPAGATION.toInt()));
                    } else {
                        setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.INIT_COUNT.toInt()));
                    }
                    break;
                case INIT_COUNT:
                    setAggregatedValue(PHASE_AGG, new IntWritable(ComputationPhase.COUNT.toInt()));
                    break;
                case COUNT:
                    break;
            }
        }
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerAggregator(MORE_ITERATIONS_AGG, BooleanOrAggregator.class);
        registerPersistentAggregator(PHASE_AGG, IntOverwriteAggregator.class);
    }

}
