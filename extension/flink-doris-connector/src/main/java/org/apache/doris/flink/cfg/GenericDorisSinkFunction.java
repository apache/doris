package org.apache.doris.flink.cfg;

import org.apache.doris.flink.table.DorisDynamicOutputFormat;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

public class GenericDorisSinkFunction<T> extends RichSinkFunction<T>
        implements CheckpointedFunction {

    private final DorisDynamicOutputFormat outputFormat;

    public GenericDorisSinkFunction(@Nonnull DorisDynamicOutputFormat outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }


    @Override
    public void invoke(T value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
        super.close();
    }

}