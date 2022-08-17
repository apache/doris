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
package org.apache.doris.demo.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/***
 *
 * Synchronize the full database through flink cdc
 *
 */
public class DatabaseFullSync {

    private static String TABLE_A = "tableA";
    private static String TABLE_B = "tableB";
    private static OutputTag<String> tableA = new OutputTag<String>(TABLE_A){};
    private static OutputTag<String> tableB = new OutputTag<String>(TABLE_B){};
    private static final Logger log = LoggerFactory.getLogger(DatabaseFullSync.class);

    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("127.0.0.1")
            .port(3306)
            .databaseList("test") // set captured database
            .tableList("test.*") // set captured table
            .username("root")
            .password("password")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);

        DataStreamSource<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        SingleOutputStreamOperator<String> process = cdcSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String row, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject rowJson = JSON.parseObject(row);
                String op = rowJson.getString("op");
                JSONObject source = rowJson.getJSONObject("source");
                String table = source.getString("table");

                //only sync insert
                if ("c".equals(op)) {
                    String value = rowJson.getJSONObject("after").toJSONString();
                    if (TABLE_A.equals(table)) {
                        context.output(tableA, value);
                    } else if (TABLE_B.equals(table)) {
                        context.output(tableB, value);
                    }
                } else {
                    log.info("other operation...");
                }
            }
        });

        //
        process.getSideOutput(tableA).sinkTo(buildDorisSink(TABLE_A));
        process.getSideOutput(tableB).sinkTo(buildDorisSink(TABLE_B));

        env.execute("Full Database Sync ");
    }


    public static DorisSink buildDorisSink(String table){
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("127.0.0.1:8030")
            .setTableIdentifier("test." + table)
            .setUsername("root")
            .setPassword("password");

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
            .setLabelPrefix("label-" + table) //streamload label prefix,
            .setStreamLoadProp(pro).build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(executionOptions)
            .setSerializer(new SimpleStringSerializer()) //serialize according to string
            .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }
}
