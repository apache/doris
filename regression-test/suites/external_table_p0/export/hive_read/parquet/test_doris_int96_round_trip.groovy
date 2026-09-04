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

suite("test_doris_int96_round_trip", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String defaultFs = "hdfs://${externalEnvIp}:${hdfsPort}"
    String uri = "${defaultFs}/user/doris/tmp_data/${UUID.randomUUID()}/exp_"

    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """
    sql """ set time_zone='Asia/Shanghai' """

    sql """ DROP TABLE IF EXISTS test_doris_int96_round_trip """
    sql """
        CREATE TABLE test_doris_int96_round_trip (
            id INT,
            datetime_value DATETIME,
            datetimev2_value DATETIMEV2(6)
        )
        DISTRIBUTED BY HASH(id)
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_doris_int96_round_trip VALUES
            (1, '2023-04-20 00:00:00', '2023-04-20 00:00:00.123456'),
            (2, '1970-01-01 08:00:00', '1970-01-01 08:00:00.654321')
    """

    def result = sql """
        SELECT * FROM test_doris_int96_round_trip ORDER BY id
        INTO OUTFILE "${uri}"
        FORMAT AS parquet
        PROPERTIES (
            "fs.defaultFS" = "${defaultFs}",
            "hadoop.username" = "doris",
            "enable_int96_timestamps" = "true"
        )
    """
    String int96OutfileUrl = result[0][3]

    // Doris normalizes INT96 using the export session timezone. Use a different read session
    // timezone to prove hive.parquet.time-zone, rather than the session, restores wall-clock time.
    sql """ set time_zone='America/Los_Angeles' """
    qt_session_timezone """ SELECT @@time_zone """
    qt_int96_round_trip """
        SELECT * FROM HDFS(
            "uri" = "${int96OutfileUrl}0.parquet",
            "hadoop.username" = "doris",
            "format" = "parquet",
            "hive.parquet.time-zone" = "Asia/Shanghai"
        )
        ORDER BY id
    """
}
