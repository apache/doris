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

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkOraclerCdcDemo {


    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);


        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " person_id INT NOT NULL,"
                                + " first_name STRING,"
                                + " last_name STRING,"
                                + "PRIMARY KEY (person_id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        "127.0.0.1",
                        1521,
                        "C##TEST",
                        "CAL0618",
                        "LHRCDB",
                        "C##TEST",
                        "persons");


        tableEnvironment.executeSql(sourceDDL);

        tableEnvironment.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "person_id INT," +
                        "first_name STRING," +
                        "last_name STRING" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = '127.0.0.1:8030',\n" +
                        "  'table.identifier' = 'test.test_oracle',\n" +
                        "  'sink.batch.size' = '2',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''\n" +
                        ")");

        tableEnvironment.executeSql("insert into doris_test_sink select person_id,first_name,last_name from debezium_source");

        env.execute();
    }
}
