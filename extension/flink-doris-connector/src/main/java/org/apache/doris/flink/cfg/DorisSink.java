package org.apache.doris.flink.cfg;

import org.apache.doris.flink.table.DorisDynamicOutputFormat;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.types.logical.LogicalType;

/** Facade to create Doris {@link SinkFunction sinks}. */
public class DorisSink {


    private DorisSink() {
    }


    /**
     * Create a Doris DataStream sink with the default {@link DorisReadOptions}
     * stream elements could only be JsonString.
     *
     * @see #sink(String[], LogicalType[], DorisReadOptions, DorisExecutionOptions, DorisOptions)
     */
    public static <T> SinkFunction<T> sink(DorisExecutionOptions executionOptions, DorisOptions dorisOptions) {

        return sink(new String[]{}, new LogicalType[]{}, DorisReadOptions.defaults(), executionOptions, dorisOptions);
    }

    /**
     * Create a Doris DataStream sink with the default {@link DorisReadOptions}
     * stream elements could only be RowData.
     *
     * @see #sink(String[], LogicalType[], DorisReadOptions, DorisExecutionOptions, DorisOptions)
     */
    public static <T> SinkFunction<T> sink(String[] fiels, LogicalType[] types,
                                           DorisExecutionOptions executionOptions, DorisOptions dorisOptions) {

        return sink(fiels, types, DorisReadOptions.defaults(), executionOptions, dorisOptions);
    }

    /**
     * Create a Doris DataStream sink with the default {@link DorisExecutionOptions}
     * stream elements could only be JsonString.
     *
     * @see #sink(String[], LogicalType[], DorisReadOptions, DorisExecutionOptions, DorisOptions)
     */
    public static <T> SinkFunction<T> sink(DorisOptions dorisOptions) {

        return sink(new String[]{}, new LogicalType[]{}, DorisReadOptions.defaults(),
                DorisExecutionOptions.defaults(), dorisOptions);
    }

    /**
     * Create a Doris DataStream sink with the default {@link DorisExecutionOptions}
     * stream elements could only be RowData.
     *
     * @see #sink(String[], LogicalType[], DorisReadOptions, DorisExecutionOptions, DorisOptions)
     */
    public static <T> SinkFunction<T> sink(String[] fiels, LogicalType[] types, DorisOptions dorisOptions) {
        return sink(fiels, types, DorisReadOptions.defaults(), DorisExecutionOptions.defaults(), dorisOptions);
    }


    /**
     * Create a Doris DataStream sink, stream elements could only be JsonString.
     *
     * @see #sink(String[], LogicalType[], DorisReadOptions, DorisExecutionOptions, DorisOptions)
     */
    public static <T> SinkFunction<T> sink(DorisReadOptions readOptions,
                                           DorisExecutionOptions executionOptions, DorisOptions dorisOptions) {

        return sink(new String[]{}, new LogicalType[]{}, readOptions, executionOptions, dorisOptions);
    }


    /**
     * Create a Doris DataStream sink, stream elements could only be RowData.
     *
     * <p>Note: the objects passed to the return sink can be processed in batch and retried.
     * Therefore, objects can not be {@link org.apache.flink.api.common.ExecutionConfig#enableObjectReuse() reused}.
     * </p>
     *
     * @param field            array of field
     * @param types            types of field
     * @param readOptions      parameters of read, such as readFields, filterQuery
     * @param executionOptions parameters of execution, such as batch size and maximum retries
     * @param dorisOptions     parameters of options, such as fenodes, username, password, tableIdentifier
     * @param <T>              type of data in {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord
     * StreamRecord}.
     */
    public static <T> SinkFunction<T> sink(String[] field, LogicalType[] types, DorisReadOptions readOptions,
                                           DorisExecutionOptions executionOptions, DorisOptions dorisOptions) {

        return new GenericDorisSinkFunction(new DorisDynamicOutputFormat(
                dorisOptions, readOptions, executionOptions, types, field));
    }

}
