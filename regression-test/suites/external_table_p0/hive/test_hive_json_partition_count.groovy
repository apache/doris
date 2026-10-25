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

suite("test_hive_json_partition_count", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    def originalScanner = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalPushdown = sql("show variables like 'enable_count_push_down_for_external_table'")[0][1]
    def originalBatchSize = sql("show variables like 'batch_size'")[0][1]
    try {
        for (String hivePrefix : ["hive2", "hive3"]) {
            setHivePrefix(hivePrefix)
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalogName = "${hivePrefix}_json_partition_count"

            hive_docker "create database if not exists regression_json_partition_count"
            hive_docker "drop table if exists regression_json_partition_count.json_partition_count"
            hive_docker """
                create table regression_json_partition_count.json_partition_count (
                    id string,
                    value string
                )
                partitioned by (etl_date string)
                row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
                stored as textfile
            """
            // All data columns are STRING. The narrower VARCHAR partition slot is the retained
            // COUNT(*) placeholder. Each populated partition spans several 1024-row batches.
            hive_docker "set hive.exec.dynamic.partition.mode=nonstrict"
            hive_docker """
                insert into regression_json_partition_count.json_partition_count partition (etl_date)
                select cast(n as string),
                       case when n % 3 = 0 then cast(null as string)
                            else concat('value-', cast(n as string)) end,
                       case when n % 7 = 0 then cast(null as string)
                            when n % 2 = 0 then '2026-10-21' else '2026-10-22' end
                from (select 1) seed
                lateral view posexplode(split(repeat('x,', 10000), ',')) generated as n, unused
            """

            sql "drop catalog if exists ${catalogName}"
            sql """
                create catalog ${catalogName} properties (
                    'type' = 'hms',
                    'hadoop.username' = 'hadoop',
                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
                )
            """

            // Compare against Hive at runtime rather than recording handwritten snapshots or
            // treating the potentially broken Doris COUNT as the expected result.
            def queries = [
                star: "select count(*) from %s",
                one: "select count(1) from %s",
                nullable_value: "select count(value) from %s",
                nullable_partition: "select count(etl_date) from %s",
                partition_filter: "select count(*) from %s where etl_date = '2026-10-21'",
                file_filter: "select count(*) from %s where value is not null",
                empty_filter: "select count(*) from %s where etl_date = 'absent'",
                grouped: "select etl_date, count(*) from %s group by etl_date order by etl_date nulls first",
                partition_rows: "select etl_date from %s where etl_date is not null order by etl_date limit 32"
            ]
            def normalize = { rows -> rows.collect { row -> row.collect { it == null ? null : it.toString() } } }
            def expected = queries.collectEntries { name, query ->
                [(name): normalize(hive_docker(String.format(query,
                    "regression_json_partition_count.json_partition_count")))]
            }
            // Reject an empty or undersized fixture: equality alone could pass without
            // exercising a populated partition and several batches.
            assertTrue(expected.star[0][0].toLong() > 4096)
            for (boolean scannerV2 : [false, true]) {
                sql "set enable_file_scanner_v2=${scannerV2}"
                for (boolean pushdown : [false, true]) {
                    sql "set enable_count_push_down_for_external_table=${pushdown}"
                    sql "set batch_size=1024"
                    queries.each { name, query ->
                        def actual = sql(String.format(query,
                            "${catalogName}.regression_json_partition_count.json_partition_count"))
                        logger.info("JSON COUNT: Hive=${hivePrefix}, V2=${scannerV2}, " +
                            "pushdown=${pushdown}, query=${name}")
                        assertEquals(expected[name], normalize(actual))
                    }
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2=${originalScanner}"
        sql "set enable_count_push_down_for_external_table=${originalPushdown}"
        sql "set batch_size=${originalBatchSize}"
    }
}
