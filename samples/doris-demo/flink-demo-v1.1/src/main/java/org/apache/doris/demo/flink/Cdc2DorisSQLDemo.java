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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Cdc2DorisSQLDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // register a table in the catalog
        tEnv.executeSql(
            "CREATE TABLE cdc_test_source (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '127.0.0.1',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                "  'database-name' = 'db',\n" +
                "  'table-name' = 'test_source'\n" +
                ")");
        //doris table
        tEnv.executeSql(
            "CREATE TABLE doris_test_sink (" +
                "id INT," +
                "name STRING" +
                ") " +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '127.0.0.1:8030',\n" +
                "  'table.identifier' = 'db.test_sink',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                /* doris stream load label, In the exactly-once scenario,
                   the label is globally unique and must be restarted from the latest checkpoint when restarting.
                   Exactly-once semantics can be turned off via sink.enable-2pc. */
                "  'sink.label-prefix' = 'doris_label',\n" +
                "  'sink.properties.format' = 'json',\n" +       //json data format
                "  'sink.properties.read_json_by_line' = 'true'\n" +
                ")");

        //insert into mysql table to doris table
        tEnv.executeSql("INSERT INTO doris_test_sink select id,name from cdc_test_source");
    }
}
