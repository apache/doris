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

suite("test_flight_int96_timestamp_range", "arrow_flight_sql,external,hive,tvf,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }
    def host = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("hive2HdfsPort")
    def flightUrl = context.getArrowFlightSqlConnection().getMetaData().getURL()
    connect(context.config.jdbcUser, context.config.jdbcPassword, flightUrl) {
        // Exercise native INT96 materialization, including legacy zero-date compatibility.
        for (def file : ["part-00000-570d8e52-652d-4892-8bdc-7fa5466ffa69.c000.snappy.parquet",
                          "part-00000-b945dfb5-9982-4f86-b903-dabef99caba1.c000.snappy.parquet",
                          "part-00000-721700d2-26d7-42a3-a8f9-b6601628ccd4.c000.snappy.parquet",
                          "int96_timestamps_nanos_outside_day_range.parquet",
                          "part-00000-afeef968-a917-4d51-a652-e5a4214df453.c000.snappy.parquet"]) {
            test {
                sql """SELECT * FROM HDFS(
                       "uri" = "hdfs://${host}:${port}/user/doris/tvf_data/test_hdfs_parquet/group4/${file}",
                       "hadoop.username" = "doris", "format" = "parquet") LIMIT 10"""
                exception "outside the supported 0001-9999 range"
            }
        }
    }
}
