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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DorisSourceSinkExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql(
                "CREATE TABLE doris_test (" +
                        "name STRING," +
                        "age INT," +
                        "price DECIMAL(5,2)," +
                        "sale DOUBLE" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'FE_IP:8030',\n" +
                        "  'table.identifier' = 'db.table',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''" +
                        ")");
        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "name STRING," +
                        "age INT," +
                        "price DECIMAL(5,2)," +
                        "sale DOUBLE" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'FE_IP:8030',\n" +
                        "  'table.identifier' = 'db.table',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '',\n" +
                        "  'sink.batch.size' = '3',\n" +
                        "  'sink.max-retries' = '2'\n" +
                        ")");

        tEnv.executeSql("INSERT INTO doris_test_sink select name,age,price,sale from doris_test");
    }
}
