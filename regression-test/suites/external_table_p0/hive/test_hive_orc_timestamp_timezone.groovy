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

suite("test_hive_orc_timestamp_timezone", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)

        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalogName = "${hivePrefix}_test_hive_orc_timestamp_timezone"
        String dbName = "test_hive_orc_timestamp_timezone"
        String simpleTable = "orc_simple_timestamp"
        String nestedTable = "orc_nested_timestamp"
        String expectedPrefix = "2020-01-02 03:04:05.321"
        String otherTimestamp = "2020-01-03 04:05:06.789"

        try {
            hive_docker """drop database if exists ${dbName} cascade"""
            hive_docker """create database ${dbName}"""
            hive_docker """set hive.local.time.zone=Asia/Shanghai"""
            hive_docker """
                create table ${dbName}.${simpleTable} (
                    id bigint,
                    ts timestamp
                )
                stored as orc
            """
            hive_docker """
                insert into ${dbName}.${simpleTable}
                select 0, cast('${expectedPrefix}' as timestamp)
                union all
                select 1, cast('${otherTimestamp}' as timestamp)
            """
            hive_docker """
                create table ${dbName}.${nestedTable} (
                    id int,
                    arr array<timestamp>,
                    ts_map map<timestamp, timestamp>,
                    row_value struct<col:timestamp>,
                    nested array<map<timestamp, struct<col:array<timestamp>>>>
                )
                stored as orc
            """
            hive_docker """
                insert into ${dbName}.${nestedTable}
                select
                    0,
                    array(cast('${expectedPrefix}' as timestamp)),
                    map(cast('${expectedPrefix}' as timestamp), cast('${expectedPrefix}' as timestamp)),
                    named_struct('col', cast('${expectedPrefix}' as timestamp)),
                    array(map(
                        cast('${expectedPrefix}' as timestamp),
                        named_struct('col', array(cast('${expectedPrefix}' as timestamp)))))
            """
            hive_docker """
                insert into ${dbName}.${nestedTable}
                select
                    1,
                    array(cast('${otherTimestamp}' as timestamp)),
                    map(cast('${otherTimestamp}' as timestamp), cast('${otherTimestamp}' as timestamp)),
                    named_struct('col', cast('${otherTimestamp}' as timestamp)),
                    array(map(
                        cast('${otherTimestamp}' as timestamp),
                        named_struct('col', array(cast('${otherTimestamp}' as timestamp)))))
            """

            sql """drop catalog if exists ${catalogName}"""
            sql """
                create catalog if not exists ${catalogName} properties (
                    'type' = 'hms',
                    'hadoop.username' = 'hadoop',
                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
                )
            """

            sql """set enable_nereids_planner=true"""
            sql """set enable_fallback_to_original_planner=false"""
            sql """set time_zone = 'CST'"""
            sql """switch ${catalogName}"""
            sql """use ${dbName}"""

            def simpleCount = sql """
                select count(*)
                from ${simpleTable}
                where id = 0
                  and cast(ts as varchar) like '${expectedPrefix}%'
            """
            assertEquals("1", simpleCount[0][0].toString())

            def nestedCount = sql """
                select count(*)
                from ${nestedTable}
                where id = 0
                  and cast(arr[0] as varchar) like '${expectedPrefix}%'
                  and cast(map_keys(ts_map)[0] as varchar) like '${expectedPrefix}%'
                  and cast(map_values(ts_map)[0] as varchar) like '${expectedPrefix}%'
                  and cast(element_at(row_value, 'col') as varchar) like '${expectedPrefix}%'
                  and cast(map_keys(nested[0])[0] as varchar) like '${expectedPrefix}%'
                  and cast(element_at(map_values(nested[0])[0], 'col')[0] as varchar)
                        like '${expectedPrefix}%'
            """
            assertEquals("1", nestedCount[0][0].toString())
        } finally {
            sql """set time_zone = default"""
            sql """switch internal"""
            sql """drop catalog if exists ${catalogName}"""
            hive_docker """drop database if exists ${dbName} cascade"""
        }
    }
}
