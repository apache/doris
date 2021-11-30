// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * example using {@link DorisSink} for streaming.
 */
public class DorisStreamSinkExample {

    /**
     * CREATE TABLE `doris_db`.`doris_table` (
     * `city` varchar(100) NOT NULL,
     * `longitude` float NOT NULL,
     * `latitude` float NOT NULL
     * )
     * UNIQUE KEY(`city`)
     * DISTRIBUTED BY HASH(`city`) BUCKETS 10
     * PROPERTIES (
     * "replication_num" = "1"
     * );
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        DorisStreamSinkExample example = new DorisStreamSinkExample();

        example.testJsonString();
        example.testJsonStringWithDefaultReadOptions();
        example.testJsonStringWithDefaultReadOptionsAndExecutionOptions();

        example.testRowData();
        example.testRowDataWithDefaultReadOptions();
        example.testRowDataWithDefaultReadOptionsAndExecutionOptions();
    }

    public void testJsonString() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties pro = new Properties();
        env.fromElements(
                        "{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}",
                        "{\"longitude\": \"116.405419\", \"city\": \"beijing\", \"latitude\": \"39.916927\"}",
                        "{\"longitude\": \"116.405419\", \"city\": \"testJsonString\", \"latitude\": \"39.916927\"}"
                )
                .addSink(
                        DorisSink.sink(
                                DorisReadOptions.builder()
                                        .build(),
                                DorisExecutionOptions.builder()
                                        .setBatchSize(3)
                                        .setBatchIntervalMs(0l)
                                        .setMaxRetries(3)
                                        .setStreamLoadProp(pro)
                                        .setDataFormat(DorisExecutionOptions.DataFormat.JSON)
                                        .setStripOuterArray(true)
                                        .setMergeType(DorisExecutionOptions.MergeType.MERGE)
                                        .setDeleteLabel("city='testJsonString'")
                                        .build(),
                                DorisOptions.builder()
                                        .setFenodes("FE_IP:8030")
                                        .setTableIdentifier("doris_db.doris_table")
                                        .setUsername("root")
                                        .setPassword("").build()
                        ));
        env.execute("doris stream sink example");
    }

    public void testJsonStringWithDefaultReadOptions() throws Exception {
        /*
         * Example for JsonString element with default ReadOptions
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties pro = new Properties();
        env.fromElements(
                        "{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}",
                        "{\"longitude\": \"116.405419\", \"city\": \"beijing\", \"latitude\": \"39.916927\"}",
                        "{\"longitude\": \"116.405419\", \"city\": \"testJsonStringWithDefaultReadOptions\", \"latitude\": \"39.916927\"}"
                )
                .addSink(
                        DorisSink.sink(
                                DorisExecutionOptions.builder()
                                        .setBatchSize(3)
                                        .setBatchIntervalMs(0l)
                                        .setMaxRetries(3)
                                        .setStreamLoadProp(pro)
                                        .setDataFormat(DorisExecutionOptions.DataFormat.JSON)
                                        .setStripOuterArray(true)
                                        .setMergeType(DorisExecutionOptions.MergeType.MERGE)
                                        .setDeleteLabel("city='testJsonStringWithDefaultReadOptions'")
                                        .build(),
                                DorisOptions.builder()
                                        .setFenodes("FE_IP:8030")
                                        .setTableIdentifier("doris_db.doris_table")
                                        .setUsername("root")
                                        .setPassword("").build()
                        ));
        env.execute("doris stream sink example");
    }


    public void testJsonStringWithDefaultReadOptionsAndExecutionOptions() throws Exception {
        /*
         * Example for JsonString element with default ReadOptions and ExecutionOptions
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                        "{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}",
                        "{\"longitude\": \"116.405419\", \"city\": \"beijing\", \"latitude\": \"39.916927\"}",
                        "{\"longitude\": \"116.405419\", \"city\": \"testJsonStringWithDefaultReadOptionsAndExecutionOptions\", \"latitude\": \"39.916927\"}"
                )
                .addSink(
                        DorisSink.sink(
                                DorisOptions.builder()
                                        .setFenodes("FE_IP:8030")
                                        .setTableIdentifier("doris_db.doris_table")
                                        .setUsername("root")
                                        .setPassword("").build()
                        ));
        env.execute("doris stream sink example");
    }


    public void testRowData() throws Exception {
        /*
         * Example for RowData element
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<RowData> source = env.fromElements("")
                .flatMap(new FlatMapFunction<String, RowData>() {
                             @Override
                             public void flatMap(String s, Collector<RowData> collector) throws Exception {
                                 GenericRowData row1 = new GenericRowData(3);
                                 row1.setField(0, StringData.fromString("北京"));
                                 row1.setField(1, 116.405419);
                                 row1.setField(2, 39.916927);
                                 collector.collect(row1);

                                 GenericRowData row2 = new GenericRowData(3);
                                 row2.setField(0, StringData.fromString("beijing"));
                                 row2.setField(1, 116.405419);
                                 row2.setField(2, 39.916927);
                                 collector.collect(row2);

                                 GenericRowData row3 = new GenericRowData(3);
                                 row3.setField(0, StringData.fromString("testRowData"));
                                 row3.setField(1, 116.405419);
                                 row3.setField(2, 39.916927);
                                 collector.collect(row3);
                             }
                         }
                );

        String[] fields = {"city", "longitude", "latitude"};
        LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};

        Properties pro = new Properties();
        source.addSink(
                DorisSink.sink(
                        fields,
                        types,
                        DorisReadOptions.builder().build(),
                        DorisExecutionOptions.builder()
                                .setBatchSize(3)
                                .setBatchIntervalMs(0L)
                                .setMaxRetries(3)
                                .setStreamLoadProp(pro)
                                .setMergeType(DorisExecutionOptions.MergeType.MERGE)
                                .setDeleteLabel("city='testRowData'")
                                .build(),
                        DorisOptions.builder()
                                .setFenodes("FE_IP:8030")
                                .setTableIdentifier("doris_db.doris_table")
                                .setUsername("root")
                                .setPassword("").build()
                ));
        env.execute("doris stream sink example");
    }


    public void testRowDataWithDefaultReadOptions() throws Exception {
        /*
         * Example for RowData element with default ReadOptions
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<RowData> source = env.fromElements("")
                .flatMap(new FlatMapFunction<String, RowData>() {
                             @Override
                             public void flatMap(String s, Collector<RowData> collector) throws Exception {
                                 GenericRowData row1 = new GenericRowData(3);
                                 row1.setField(0, StringData.fromString("北京"));
                                 row1.setField(1, 116.405419);
                                 row1.setField(2, 39.916927);
                                 collector.collect(row1);

                                 GenericRowData row2 = new GenericRowData(3);
                                 row2.setField(0, StringData.fromString("beijing"));
                                 row2.setField(1, 116.405419);
                                 row2.setField(2, 39.916927);
                                 collector.collect(row2);

                                 GenericRowData row3 = new GenericRowData(3);
                                 row3.setField(0, StringData.fromString("testRowDataWithDefaultReadOptions"));
                                 row3.setField(1, 116.405419);
                                 row3.setField(2, 39.916927);
                                 collector.collect(row3);
                             }
                         }
                );

        String[] fields = {"city", "longitude", "latitude"};
        LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};

        Properties pro = new Properties();
        source.addSink(
                DorisSink.sink(
                        fields,
                        types,
                        DorisExecutionOptions.builder()
                                .setBatchSize(3)
                                .setBatchIntervalMs(0L)
                                .setMaxRetries(3)
                                .setStreamLoadProp(pro)
                                .setMergeType(DorisExecutionOptions.MergeType.MERGE)
                                .setDeleteLabel("city='testRowDataWithDefaultReadOptions'")
                                .build(),
                        DorisOptions.builder()
                                .setFenodes("FE_IP:8030")
                                .setTableIdentifier("doris_db.doris_table")
                                .setUsername("root")
                                .setPassword("").build()
                ));
        env.execute("doris stream sink example");
    }


    public void testRowDataWithDefaultReadOptionsAndExecutionOptions() throws Exception {
        /*
         * Example for RowData element with default ReadOptions and ExecutionOptions
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<RowData> source = env.fromElements("")
                .flatMap(new FlatMapFunction<String, RowData>() {
                             @Override
                             public void flatMap(String s, Collector<RowData> collector) throws Exception {
                                 GenericRowData row1 = new GenericRowData(3);
                                 row1.setField(0, StringData.fromString("北京"));
                                 row1.setField(1, 116.405419);
                                 row1.setField(2, 39.916927);
                                 collector.collect(row1);

                                 GenericRowData row2 = new GenericRowData(3);
                                 row2.setField(0, StringData.fromString("beijing"));
                                 row2.setField(1, 116.405419);
                                 row2.setField(2, 39.916927);
                                 collector.collect(row2);

                                 GenericRowData row3 = new GenericRowData(3);
                                 row3.setField(0, StringData.fromString("testRowDataWithDefaultReadOptionsAndExecutionOptions"));
                                 row3.setField(1, 116.405419);
                                 row3.setField(2, 39.916927);
                                 collector.collect(row3);
                             }
                         }
                );

        String[] fields = {"city", "longitude", "latitude"};
        LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};

        source.addSink(
                DorisSink.sink(
                        fields,
                        types,
                        DorisOptions.builder()
                                .setFenodes("FE_IP:8030")
                                .setTableIdentifier("doris_db.doris_table")
                                .setUsername("root")
                                .setPassword("").build()
                ));
        env.execute("doris stream sink example");
    }
}