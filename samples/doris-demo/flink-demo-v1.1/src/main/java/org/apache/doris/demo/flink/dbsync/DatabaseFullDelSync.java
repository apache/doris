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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;


/***
 *
 * Synchronize the full database through flink cdc
 *
 */
public class DatabaseFullDelSync {

    private static String SOURCE_DB = "custom_db";//db
    private static String SOURCE_TABLS = "custom_db.test_flink_doris"; //tables
    private static String SOURCE_IP = "127.0.0.1"; //hostname
    private static String SOURCE_USER = "root"; //username
    private static String SOURCE_PWD = "root"; //password
    private static int SOURCE_PORT = 3306;


    private static String DORIS_IP = "127.0.0.1";
    private static String DORIS_PORT = "8030";
    private static String DORIS_USER = "root";
    private static String DORIS_PWD = "root";
    private static String DORIS_DB = "table_structure.test_flink_doris";
    private static final Logger log = LoggerFactory.getLogger(DatabaseFullDelSync.class);

    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname(SOURCE_IP)
            .port(SOURCE_PORT)
            .databaseList(SOURCE_DB) // set captured database
            .tableList(SOURCE_TABLS) // set captured table
            .username(SOURCE_USER) // set captured user
            .password(SOURCE_PWD) // set captured pwd
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);
        DataStreamSource<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
        //get table list
        List<String> tableList = getTableList();
        if (null != tableList && tableList.size() > 0) {
            //get column map
            Map<String, String> tableColumn = getTableColumn();
            for (String tbl : tableList) {
                String column = tableColumn.get(tbl);
                SingleOutputStreamOperator fifterStream = fifterTableData(cdcSource, tbl);
                SingleOutputStreamOperator<String> cleanStream = cleanData(fifterStream);
                DorisSink dorisSink = buildDorisSink(tbl, column);
                cleanStream.sinkTo(dorisSink).name(tbl);
            }
            env.execute("Full Database Sync");
        }

    }


    /**
     * Get real data
     * {
     * "before":null,
     * "after":{
     * "id":1,
     * "name":"zhangsan-1",
     * "age":18
     * },
     * "source":{
     * "db":"test",
     * "table":"test_1",
     * ...
     * },
     * "op":"c",
     * ...
     * }
     */
    public static SingleOutputStreamOperator fifterTableData(DataStreamSource<String> cdcSource, String table) {
        return cdcSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String row) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    JSONObject source = rowJson.getJSONObject("source");
                    String tbl = source.getString("table");
                    return table.equals(tbl);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return false;
                }
            }
        });
    }


    //cleanData
    public static SingleOutputStreamOperator<String> cleanData(SingleOutputStreamOperator source) {
        return source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String row, Collector<String> out) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    String op = rowJson.getString("op");
                    if (Arrays.asList("c", "r", "u").contains(op)) {
                        JSONObject after = rowJson.getJSONObject("after");
                        after.put("__DORIS_DELETE_SIGN__", false);
                        out.collect(after.toJSONString());
                    } else if ("d".equals(op)) {
                        JSONObject before = rowJson.getJSONObject("before");
                        before.put("__DORIS_DELETE_SIGN__", true);
                        out.collect(before.toJSONString());
                    } else {
                        log.info("filter other op:{}", op);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    log.warn("filter other format binlog:{} ", row);
                }
            }
        });
    }

    // create doris sink
    public static DorisSink buildDorisSink(String table, String tableColumn) {
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(DORIS_IP + ":" + DORIS_PORT)
            .setTableIdentifier(DORIS_DB)
            .setUsername(DORIS_USER)
            .setPassword(DORIS_PWD);

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        //delete use
        pro.setProperty("columns", tableColumn + ",`__DORIS_DELETE_SIGN__`");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
            .setLabelPrefix("label-" + System.currentTimeMillis() + table) //streamload label prefix,
            .setStreamLoadProp(pro)
            .setDeletable(true)
            .build();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(executionOptions)
            .setSerializer(new SimpleStringSerializer()) //serialize according to string
            .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }


    public static List<String> getTableList() {
        List<String> list = new ArrayList<>();
        String sql = "SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.tables  WHERE TABLE_SCHEMA = '" + SOURCE_DB + "'";
        try {
            List<JSONObject> JSONObject = JdbcUtil.executeQuery(SOURCE_IP, SOURCE_PORT, SOURCE_USER, SOURCE_PWD, sql);
            if (null != JSONObject && JSONObject.size() > 0) {
                for (JSONObject json : JSONObject) {
                    String tableSchema = json.getString("TABLE_SCHEMA");
                    String tableName = json.getString("TABLE_NAME");
                    String dbTable = tableSchema + "." + tableName;
                    if (dbTable.matches(SOURCE_TABLS)) {
                        list.add(tableName);
                    }
                }
                return list;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }


    public static Map<String, String> getTableColumn() {
        Map<String, String> reMap = new HashMap<>();
        String sql = "select TABLE_SCHEMA,TABLE_NAME,GROUP_CONCAT('`',COLUMN_NAME,'`') AS columnStr from information_schema.columns" +
            " WHERE TABLE_SCHEMA =  '" + SOURCE_DB + "'" + " GROUP BY TABLE_SCHEMA,TABLE_NAME";
        try {
            List<JSONObject> JSONObject = JdbcUtil.executeQuery(SOURCE_IP, SOURCE_PORT, SOURCE_USER, SOURCE_PWD, sql);
            if (null != JSONObject && JSONObject.size() > 0) {
                for (JSONObject json : JSONObject) {
                    String tableSchema = json.getString("TABLE_SCHEMA");
                    String tableName = json.getString("TABLE_NAME");
                    String columnStr = json.getString("columnStr");
                    String dbTable = tableSchema + "." + tableName;
                    if (dbTable.matches(SOURCE_TABLS)) {
                        reMap.put(tableName, columnStr);
                    }
                }
                return reMap;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }

}
