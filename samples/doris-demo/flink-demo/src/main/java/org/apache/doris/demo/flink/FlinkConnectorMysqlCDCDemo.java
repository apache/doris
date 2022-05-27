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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Realize the consumption of mysql binlog log data through flink cdc,
 * and then import mysql data into doris table data in real time through flink doris connector sql
 */
public class FlinkConnectorMysqlCDCDemo {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // source only supports parallelism of 1
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // register a table in the catalog
        tEnv.executeSql(
                "CREATE TABLE orders (\n" +
                        "  id INT,\n" +
                        "  name STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = 'localhost',\n" +
                        "  'port' = '3306',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'zhangfeng',\n" +
                        "  'database-name' = 'demo',\n" +
                        "  'table-name' = 'test'\n" +
                        ")");
        //doris table
        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "id INT," +
                        "name STRING" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = '10.220.146.10:8030',\n" +
                        "  'table.identifier' = 'test_2.doris_test',\n" +
                        "  'sink.batch.size' = '2',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''\n" +
                        ")");
        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("SELECT id,name FROM orders");
        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();
        result.execute();

        //insert into mysql table to doris table
        tEnv.executeSql("INSERT INTO doris_test_sink select id,name from orders");
        env.execute();
    }
}
