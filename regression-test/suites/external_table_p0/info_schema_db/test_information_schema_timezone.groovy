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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

suite("test_information_schema_timezone", "p0,external,hive,kerberos,external_docker,external_docker_kerberos") {
    def table_name = "test_information_schema_timezone"
    def originalTimeZone = sql("select @@time_zone")[0][0]
    try {
        sql """ SET time_zone = "Asia/Shanghai" """
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `id` int(11) NULL,
            `name` string NULL,
            `age` int(11) NULL
            )
            PARTITION BY RANGE(id)
            (
                PARTITION less_than_20 VALUES LESS THAN ("20"),
                PARTITION between_20_70 VALUES [("20"),("70")),
                PARTITION more_than_70 VALUES LESS THAN ("151")
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i}, 'ftw-${i}', ${i + 18}),
            """)
        }
        sb.append("""
                (${i}, NULL, NULL)
            """)
        sql """ INSERT INTO ${table_name} VALUES
                ${sb.toString()}
            """
        qt_select_export """ SELECT * FROM ${table_name} t ORDER BY id; """

        def isEightHoursDiff = { LocalDateTime shanghaiTime, LocalDateTime utcTime ->
            // Comparing the same event must include the date and sub-hour fields, even across midnight.
            return shanghaiTime != null && utcTime != null && shanghaiTime.minusHours(8) == utcTime
        }

        def getStableRowsetsTime = { String predicate ->
            List<List<Object>> result = null
            awaitUntil(30) {
                result = sql """
                    select BACKEND_ID, TABLET_ID, ROWSET_ID, CREATION_TIME, NEWEST_WRITE_TIMESTAMP
                    from information_schema.rowsets
                    where ${predicate}
                    order by BACKEND_ID, TABLET_ID, ROWSET_ID
                    limit 1
                """
                logger.info("stable rowsets predicate = ${predicate}, result = ${result}")
                return result != null && !result.isEmpty()
            }
            return result
        }

        def assertTimeInWindow = { LocalDateTime time, int offsetHours, LocalDateTime before, LocalDateTime after ->
            LocalDateTime utcTime = time.minusHours(offsetHours)
            assertTrue(!utcTime.isBefore(before) && !utcTime.isAfter(after),
                    "Timestamp ${utcTime} is outside FE time window [${before}, ${after}]")
        }

        def checkProcedureTime = { String procedureName, int offsetHours ->
            // Procedure timestamps are stored as strings in the creation session's time zone.
            // Check each creation event independently; reading it later does not convert its zone.
            sql """ DROP PROC ${procedureName} """
            LocalDateTime before = sql("select utc_timestamp()")[0][0]
            sql """ CREATE OR REPLACE PROCEDURE ${procedureName}() BEGIN DECLARE a int = 1; print a; END; """
            def result = sql """
                select CREATED, LAST_ALTERED from information_schema.routines
                where ROUTINE_SCHEMA = database() and ROUTINE_NAME = '${procedureName.toUpperCase(Locale.ROOT)}'
            """
            LocalDateTime after = sql("select utc_timestamp()")[0][0]
            assertEquals(1, result.size())
            result[0].each { timestamp -> assertTimeInWindow(timestamp, offsetHours, before, after) }
        }

        def getLoginTime = {
            // Connection IDs are local to each FE, while processlist can include every FE.
            String marker = UUID.randomUUID().toString()
            def result = sql """
                select LOGINTIME from information_schema.processlist
                where ID = connection_id() and INFO like '%${marker}%'
            """
            assertEquals(1, result.size())
            return result[0][0]
        }

        def checkActiveQueryTime = { int offsetHours ->
            // These queries are different events. Bound each timestamp with FE time instead of
            // assuming that queries in two time zones start in the same minute or hour.
            LocalDateTime before = sql("select utc_timestamp()")[0][0]
            String marker = UUID.randomUUID().toString()
            def result = sql """
                select QUERY_START_TIME from information_schema.active_queries
                where SQL like '%${marker}%'
            """
            LocalDateTime after = sql("select utc_timestamp()")[0][0]
            assertEquals(1, result.size())
            LocalDateTime start = LocalDateTime.parse(result[0][0],
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            assertTimeInWindow(start, offsetHours, before, after)
        }

        // 0. Assert the timezone
        qt_session_time_zone_UTC """ show variables like "time_zone" """

        // 1. tables
        List<List<Object>> tables_res_1 = sql """ select CREATE_TIME, UPDATE_TIME from information_schema.tables where TABLE_SCHEMA = database() and TABLE_NAME = "${table_name}" """
        logger.info("tables_res_1 = " + tables_res_1);

        // 2. routines
        checkProcedureTime("test_information_schema_timezone1", 8)

        // 3. partitions
        List<List<Object>> partitions_res_1 = sql """ select UPDATE_TIME from information_schema.partitions where TABLE_SCHEMA = database() and TABLE_NAME = "${table_name}" and PARTITION_NAME = "less_than_20" """
        logger.info("partitions_res_1 = " + partitions_res_1);

        // 4. processlist
        LocalDateTime processlist_res_1 = getLoginTime()
        logger.info("processlist_res_1 = " + processlist_res_1);

        // 5. rowsets
        // Cloud rowsets only exposes cached tablets. Empty partitions can be pruned from the
        // preceding scan, so choose a visible data rowset from the partition we actually populated.
        def rowsets_table_name_tablets = sql_return_maparray """
            show tablets from ${table_name} partition(less_than_20)
        """
        def tabletIds = rowsets_table_name_tablets.collect { it.TabletId }.unique().join(",")
        List<List<Object>> rowsets_res_1 = getStableRowsetsTime(
                "TABLET_ID in (${tabletIds}) and ROWSET_NUM_ROWS > 0")
        def rowset = rowsets_res_1[0]
        String rowsetPredicate = "BACKEND_ID = ${rowset[0]} and TABLET_ID = ${rowset[1]} and ROWSET_ID = '${rowset[2]}'"
        logger.info("rowsets_res_1 = " + rowsets_res_1);

        // 6. backend_kerberos_ticket_cache
        // TODO(ftw): Since there are some problems in kerveros case, we don't test this case tempraturely.
        // String enabled = context.config.otherConfigs.get("enableKerberosTest")
        String enabled = false
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        logger.info("enableKerberosTest = " + enabled + " ; externalEnvIp = " + externalEnvIp);
        List<List<Object>> kerberos_cache_res_1 = null
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            sql """
                CREATE CATALOG IF NOT EXISTS test_information_schema_timezone_catalog
                PROPERTIES (
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
                    "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
                    "dfs.namenode.kerberos.principal" = "hdfs/hadoop-master@LABS.TERADATA.COM",
                    "dfs.client.use.datanode.hostname" = "true",
                    "hadoop.security.token.service.use_ip" = "false",
                    "hadoop.kerberos.min.seconds.before.relogin" = "5",
                    "hadoop.security.authentication" = "kerberos",
                    "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                    "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
                    "hive.metastore.kerberos.principal" = "hive/hadoop-master@LABS.TERADATA.COM"
                );
            """

            sql """ DROP TABLE IF EXISTS test_information_schema_timezone_catalog.test_krb_hive_db.test_information_schema_table """
            sql """ CREATE TABLE test_information_schema_timezone_catalog.test_krb_hive_db.test_information_schema_table (id int, str string, dd date) engine = hive; """
            sql """ INSERT INTO test_information_schema_timezone_catalog.test_krb_hive_db.test_information_schema_table values(1, 'krb1', '2023-05-14') """
            sql """ INSERT INTO test_information_schema_timezone_catalog.test_krb_hive_db.test_information_schema_table values(2, 'krb2', '2023-05-24') """

            order_qt_select_kerberos """ select * from test_information_schema_timezone_catalog.test_krb_hive_db.test_information_schema_table; """

            kerberos_cache_res_1 = sql """
                        select START_TIME, EXPIRE_TIME, AUTH_TIME from information_schema.backend_kerberos_ticket_cache where PRINCIPAL="hive/presto-master.docker.cluster@LABS.TERADATA.COM" and KEYTAB = "${keytab_root_dir}/hive-presto-master.keytab"
                    """
            logger.info("kerberos_cache_res_1 = " + kerberos_cache_res_1);

        }

        // 7. active_queries
        checkActiveQueryTime(8)

        // ------------------- change the time zone
        sql """ SET time_zone = "UTC" """
        qt_session_time_zone_UTC  """ show variables like "time_zone" """

        // 1. tables
        List<List<Object>> tables_res_2 = sql """ select CREATE_TIME, UPDATE_TIME from information_schema.tables where TABLE_SCHEMA = database() and TABLE_NAME = "${table_name}" """
        logger.info("tables_res_2 = " + tables_res_2);
        assertEquals(true, isEightHoursDiff(tables_res_1[0][0], tables_res_2[0][0]))
        assertEquals(true, isEightHoursDiff(tables_res_1[0][1], tables_res_2[0][1]))

        // 2. routines
        checkProcedureTime("test_information_schema_timezone2", 0)

        // 3. partitions
        List<List<Object>> partitions_res_2 = sql """ select UPDATE_TIME from information_schema.partitions where TABLE_SCHEMA = database() and TABLE_NAME = "${table_name}" and PARTITION_NAME = "less_than_20" """
        logger.info("partitions_res_2 = " + partitions_res_2);
        assertEquals(true, isEightHoursDiff(partitions_res_1[0][0], partitions_res_2[0][0]))

        // 4. processlist
        LocalDateTime processlist_res_2 = getLoginTime()
        logger.info("processlist_res_2 = " + processlist_res_2);
        assertEquals(true, isEightHoursDiff(processlist_res_1, processlist_res_2))

        // 5. rowsets
        List<List<Object>> rowsets_res_2 = getStableRowsetsTime(rowsetPredicate)
        logger.info("rowsets_res_2 = " + rowsets_res_2);
        assertEquals(rowsets_res_1[0][0..2], rowsets_res_2[0][0..2])
        assertEquals(true, isEightHoursDiff(rowsets_res_1[0][3], rowsets_res_2[0][3]))
        if (rowsets_res_1[0][4] == null || rowsets_res_2[0][4] == null) {
            assertEquals(rowsets_res_1[0][4], rowsets_res_2[0][4])
        } else {
            assertEquals(true, isEightHoursDiff(rowsets_res_1[0][4], rowsets_res_2[0][4]))
        }

        // 6. backend_kerberos_ticket_cache
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            List<List<Object>> kerberos_cache_res_2 = sql """
                    select START_TIME, EXPIRE_TIME, AUTH_TIME from information_schema.backend_kerberos_ticket_cache where PRINCIPAL="hive/presto-master.docker.cluster@LABS.TERADATA.COM" and KEYTAB = "${keytab_root_dir}/hive-presto-master.keytab"
                """
            logger.info("kerberos_cache_res_2 = " + kerberos_cache_res_2);

            assertEquals(true, isEightHoursDiff(kerberos_cache_res_1[0][0], kerberos_cache_res_2[0][0]))
            assertEquals(true, isEightHoursDiff(kerberos_cache_res_1[0][1], kerberos_cache_res_2[0][1]))
            assertEquals(true, isEightHoursDiff(kerberos_cache_res_1[0][2], kerberos_cache_res_2[0][2]))

            sql """DROP CATALOG IF EXISTS test_information_schema_timezone_catalog"""
        }

        // 7. active_queries
        checkActiveQueryTime(0)

        // set time_zone back
        sql """ SET time_zone = "Asia/Shanghai" """

        // 8. Test offset format timezone (e.g. +08:00)
        // This tests the fix for cctz returning "Fixed/UTC+08:00:00" format
        sql """ SET time_zone = '+08:00' """
        sql """
            select a.*, b.*, c.NAME as WORKLOAD_GROUP_NAME
            from information_schema.active_queries a
            left join information_schema.backend_active_tasks b on a.QUERY_ID = b.QUERY_ID
            left join information_schema.workload_groups c on a.WORKLOAD_GROUP_ID = c.ID
        """
        sql """ SET time_zone = "Asia/Shanghai" """
    } finally {
        sql """ SET time_zone = '${originalTimeZone}' """
        sql """ DROP PROC test_information_schema_timezone1 """
        sql """ DROP PROC test_information_schema_timezone2 """
        sql """ DROP TABLE IF EXISTS ${table_name} """
    }
}
