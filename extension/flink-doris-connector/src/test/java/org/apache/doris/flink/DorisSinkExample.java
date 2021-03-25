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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class DorisSinkExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //streamload default delimiter: \t
        DataStreamSource<String> source = env.fromElements("[{\"name\":\"doris\"}]\t1");
        tEnv.createTemporaryView("doris_test",source,$("name"));

//        Table wordWithCount = tEnv.sqlQuery("select name FROM doris_test  ");
//        tEnv.toRetractStream(wordWithCount, Row.class).print();

        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "name STRING" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'FE_IP:8030',\n" +
                        "  'table.identifier' = 'ods.doris_test_sink_1',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''\n" +
                        ")");


        tEnv.executeSql("INSERT INTO doris_test_sink select name from doris_test");
    }
}
