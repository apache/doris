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
package org.apache.doris.demo.flink.dbsync;

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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/***
 *
 * Synchronize the full database through flink cdc
 *
 */
public class DatabaseFullSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseFullSync.class);
    private static String HOST = "127.0.0.1";
    private static String MYSQL_PASSWD = "password";
    private static int MYSQL_PORT = 3306;
    private static int DORIS_PORT = 8030;
    private static String MYSQL_USER = "root";


    private static String SYNC_DB = "test";
    private static String SYNC_TBLS = "test.*";
    private static String TARGET_DORIS_DB = "test";

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname(HOST)
            .port(MYSQL_PORT)
            .databaseList(SYNC_DB) // set captured database
            .tableList(SYNC_TBLS) // set captured table
            .username(MYSQL_USER)
            .password(MYSQL_PASSWD)
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);

        DataStreamSource<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        //get table list
        List<String> tableList = getTableList();
        LOG.info("sync table list:{}",tableList);
        for(String tbl : tableList){
            SingleOutputStreamOperator<String> filterStream = filterTableData(cdcSource, tbl);
            SingleOutputStreamOperator<String> cleanStream = clean(filterStream);
            DorisSink dorisSink = buildDorisSink(tbl);
            cleanStream.sinkTo(dorisSink).name("sink " + tbl);
        }
        env.execute("Full Database Sync ");
    }

    /**
     * Get real data
     * {
     *     "before":null,
     *     "after":{
     *         "id":1,
     *         "name":"zhangsan-1",
     *         "age":18
     *     },
     *     "source":{
     *         "db":"test",
     *         "table":"test_1",
     *         ...
     *     },
     *     "op":"c",
     *     ...
     * }
     * */
    private static SingleOutputStreamOperator<String> clean(SingleOutputStreamOperator<String> source) {
        return source.flatMap(new FlatMapFunction<String,String>(){
            @Override
            public void flatMap(String row, Collector<String> out) throws Exception {
                try{
                    JSONObject rowJson = JSON.parseObject(row);
                    String op = rowJson.getString("op");
                    //history,insert,update
                    if(Arrays.asList("r","c","u").contains(op)){
                        out.collect(rowJson.getJSONObject("after").toJSONString());
                    }else{
                        LOG.info("filter other op:{}",op);
                    }
                }catch (Exception ex){
                    LOG.warn("filter other format binlog:{}",row);
                }
            }
        });
    }

    /**
     * Divide according to tablename
     * */
    private static SingleOutputStreamOperator<String> filterTableData(DataStreamSource<String> source, String table) {
        return source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String row) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    JSONObject source = rowJson.getJSONObject("source");
                    String tbl = source.getString("table");
                    return table.equals(tbl);
                }catch (Exception ex){
                    ex.printStackTrace();
                    return false;
                }
            }
        });
    }

    /**
     * Get all MySQL tables that need to be synchronized
     * */
    private static List<String> getTableList() {
        List<String> tables = new ArrayList<>();
        String sql = "SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '" + SYNC_DB + "'";
        List<JSONObject> tableList = JdbcUtil.executeQuery(HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWD, sql);
        for(JSONObject jsob : tableList){
            String schemaName = jsob.getString("TABLE_SCHEMA");
            String tblName = jsob.getString("TABLE_NAME");
            String schemaTbl = schemaName  + "." + tblName;
            if(schemaTbl.matches(SYNC_TBLS)){
                tables.add(tblName);
            }
        }
        return tables;
    }

    /**
     * create doris sink
     * */
    public static DorisSink buildDorisSink(String table){
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(HOST + ":" + DORIS_PORT)
            .setTableIdentifier(TARGET_DORIS_DB + "." + table)
            .setUsername("root")
            .setPassword("");

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
            .setLabelPrefix("label-" + table + UUID.randomUUID()) //streamload label prefix,
            .setStreamLoadProp(pro).build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(executionOptions)
            .setSerializer(new SimpleStringSerializer()) //serialize according to string
            .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }
}
