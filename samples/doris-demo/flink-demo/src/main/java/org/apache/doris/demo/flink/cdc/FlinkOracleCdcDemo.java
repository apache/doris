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
package org.apache.doris.demo.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.oracle.OracleSource;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Properties;

/**
 *Realize the consumption of oracle log data through flink cdc,
 * and then import oracle data into doris table data in real time
 * through the flink doris connector RowData data stream;
 */
public class FlinkOracleCdcDemo {

    private static final String DATABASE_NAME = "xxx";

    private static final String HOST_NAME = "127.0.0.1";

    private static final int PORT = 1521;

    private static final String SCHEMA_NAME = "xxx";

    private static final String TABLE_NAME = "schema_name.table_name";

    private static final String USER_NAME = "xxx";

    private static final String PASSWORD = "xxx";


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.setProperty("debezium.database.tablename.case.insensitive","false");
        props.setProperty("debezium.log.mining.strategy","online_catalog");
        props.setProperty("debezium.log.mining.continuous.mine","true");


        SourceFunction<JSONObject> build = OracleSource.<JSONObject>builder()
                .database(DATABASE_NAME)
                .hostname(HOST_NAME)
                .port(PORT)
                .username(USER_NAME)
                .password(PASSWORD)
                .schemaList(SCHEMA_NAME)
                .tableList(TABLE_NAME)
                .debeziumProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<JSONObject> dataStreamSource = env.addSource(build);


        SingleOutputStreamOperator<RowData> map = dataStreamSource.map(new MapFunction<JSONObject, RowData>() {
            @Override
            public RowData map(JSONObject jsonObject) throws Exception {
                GenericRowData genericRowData = new GenericRowData(4);
                genericRowData.setField(0, Integer.valueOf(jsonObject.getInteger("id")));
                genericRowData.setField(1, StringData.fromString(jsonObject.getString("name")));
                genericRowData.setField(2, StringData.fromString(jsonObject.getString("description")));
                genericRowData.setField(3, Double.valueOf(jsonObject.getDoubleValue("weight")));
                return genericRowData;
            }
        });


        String[] fields = {"id", "name", "description","weight"};

        LogicalType[] types={new IntType(),new VarCharType(),new VarCharType(), new DoubleType()};

        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "false");

        map.addSink(
                DorisSink.sink(
                        fields,
                        types,
                        DorisReadOptions.builder().build(),
                        DorisExecutionOptions.builder()
                                .setBatchSize(3)
                                .setMaxRetries(3)

                                .build(),
                        DorisOptions.builder()
                                .setFenodes("127.0.0.1:8030")
                                .setTableIdentifier("db_name.table_name")
                                .setUsername("root")
                                .setPassword("").build()
                ));

        env.execute("flink oracle cdc");
    }
}
