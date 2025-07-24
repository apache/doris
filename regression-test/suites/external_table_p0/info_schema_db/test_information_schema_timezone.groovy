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
    /* 
     * In this test case, we compare the intervals between two time zones. 
     *      List<List<Object>> routines_res_1 = sql """ select CREATED,LAST_ALTERED from information_schema.routines where ROUTINE_NAME="TEST_INFORMATION_SCHEMA_TIMEZONE1" """
     *      List<List<Object>> routines_res_2 = sql """ select CREATED,LAST_ALTERED from information_schema.routines where ROUTINE_NAME="TEST_INFORMATION_SCHEMA_TIMEZONE2"; """
     *      assertEquals(true, isEightHoursDiff(tables_res_2[0][0], tables_res_1[0][0]))
     * If the two SQL queries are executed across the hour mark, it will cause the test case to fail.
     * eg. routines_res_1 executed at 00:59:50, and routines_res_2 executed at 01:00:05, assert will fail.
     * The execution time of this test case is within 2 minutes, and the interval between the two queries is about 20 seconds. 
     */
    waitUntilSafeExecutionTime("NOT_CROSS_HOUR_BOUNDARY", 45)

    def table_name = "test_information_schema_timezone"
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

    def isEightHoursDiff = {LocalDateTime dateTime1, LocalDateTime dateTime2 -> 
        logger.info("dateTime1 = " + dateTime1 + " ; dateTime2 = " + dateTime2)
        try {
            int hour1 = dateTime1.getHour();
            int hour2 = dateTime2.getHour();
            logger.info("hour1 = " + hour1 + " ; hour2 = " + hour2)
            

            int difference = Math.abs(hour1 - hour2);
            return difference == 8 || difference == 24 - 8;
        } catch (Exception e) {
            throw exception("Wrong datetime formatter: " + e.getMessage());
        }
    }

    // 0. Assert the timezone
    qt_session_time_zone_UTC """ show variables like "time_zone" """

    // 1. tables
    List<List<Object>> tables_res_1 = sql """ select CREATE_TIME, UPDATE_TIME from information_schema.tables where TABLE_NAME = "${table_name}" """
    logger.info("tables_res_1 = " + tables_res_1);

    // 2. routines
    sql """DROP PROC test_information_schema_timezone1"""
    def procedure_body = "BEGIN DECLARE a int = 1; print a; END;"
    sql """ CREATE OR REPLACE PROCEDURE test_information_schema_timezone1() ${procedure_body} """
    List<List<Object>> routines_res_1 = sql """ select CREATED,LAST_ALTERED from information_schema.routines where ROUTINE_NAME="TEST_INFORMATION_SCHEMA_TIMEZONE1" """
    logger.info("routines_res_1 = " + routines_res_1);
    sql """DROP PROC test_information_schema_timezone1"""

    // 3. partitions
    List<List<Object>> partitions_res_1 = sql """ select UPDATE_TIME from information_schema.partitions where TABLE_NAME = "${table_name}" """
    logger.info("partitions_res_1 = " + partitions_res_1);

    // 4. processlist
    List<List<Object>> processlist_res_1 = sql """ 
            select LOGINTIME from information_schema.processlist where INFO like "%information_schema.processlist%"
            """
    logger.info("processlist_res_1 = " + processlist_res_1);
    
    // 5. rowsets
    def rowsets_table_name_tablets = sql_return_maparray """ show tablets from ${table_name}; """
    def tablet_id = rowsets_table_name_tablets[0].TabletId
    List<List<Object>> rowsets_res_1 = sql """ 
            select CREATION_TIME, NEWEST_WRITE_TIMESTAMP from information_schema.rowsets where TABLET_ID = ${tablet_id}
            """
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
                "hadoop.kerberos.min.seconds.before.relogin" = "5",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
                "hive.metastore.sasl.enabled " = "true",
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
    List<List<Object>> active_queries_res_1 = sql """ 
                select QUERY_START_TIME from information_schema.active_queries where SQL like "%information_schema.active_queries%"
            """
    logger.info("active_queries_res_1 = " + active_queries_res_1);

    // ------------------- change the time zone
    sql """ SET time_zone = "UTC" """
    qt_session_time_zone_UTC  """ show variables like "time_zone" """

    // 1. tables
    List<List<Object>> tables_res_2 = sql """ select CREATE_TIME, UPDATE_TIME from information_schema.tables where TABLE_NAME = "${table_name}" """
    logger.info("tables_res_2 = " + tables_res_2);
    assertEquals(true, isEightHoursDiff(tables_res_2[0][0], tables_res_1[0][0]))
    assertEquals(true, isEightHoursDiff(tables_res_2[0][1], tables_res_1[0][1]))

    // 2. routines
    sql """DROP PROC test_information_schema_timezone2"""
    def procedure_body2 = "BEGIN DECLARE a int = 1; print a; END;"
    sql """ CREATE OR REPLACE PROCEDURE test_information_schema_timezone2() ${procedure_body2} """
    List<List<Object>> routines_res_2 = sql """ select CREATED,LAST_ALTERED from information_schema.routines where ROUTINE_NAME="TEST_INFORMATION_SCHEMA_TIMEZONE2"; """
    logger.info("routines_res_2 = " + routines_res_2);
    sql """DROP PROC test_information_schema_timezone2"""
    assertEquals(true, isEightHoursDiff(routines_res_1[0][0], routines_res_2[0][0]))
    assertEquals(true, isEightHoursDiff(routines_res_1[0][1], routines_res_2[0][1]))
    
    
    // 3. partitions
    List<List<Object>> partitions_res_2 = sql """ select UPDATE_TIME from information_schema.partitions where TABLE_NAME = "${table_name}" """
    logger.info("partitions_res_2 = " + partitions_res_2);
    assertEquals(true, isEightHoursDiff(partitions_res_1[0][0], partitions_res_2[0][0]))

    // 4. processlist
    List<List<Object>> processlist_res_2 = sql """ 
            select LOGINTIME from information_schema.processlist where INFO like "%information_schema.processlist%"
            """
    logger.info("processlist_res_2 = " + processlist_res_2);
    assertEquals(true, isEightHoursDiff(processlist_res_1[0][0], processlist_res_2[0][0]))

    // 5. rowsets
    List<List<Object>> rowsets_res_2 = sql """ 
            select CREATION_TIME, NEWEST_WRITE_TIMESTAMP from information_schema.rowsets where TABLET_ID = ${tablet_id}
            """
    logger.info("rowsets_res_2 = " + rowsets_res_2);
    assertEquals(true, isEightHoursDiff(rowsets_res_1[0][0], rowsets_res_2[0][0]))
    assertEquals(true, isEightHoursDiff(rowsets_res_1[0][1], rowsets_res_2[0][1]))

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
    List<List<Object>> active_queries_res_2 = sql """ 
                select QUERY_START_TIME from information_schema.active_queries where SQL like "%information_schema.active_queries%"
            """
    logger.info("active_queries_res_2 = " + active_queries_res_2);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    try {
        LocalDateTime dateTime1 = LocalDateTime.parse(active_queries_res_1[0][0], formatter);
        LocalDateTime dateTime2 = LocalDateTime.parse(active_queries_res_2[0][0], formatter);
        assertEquals(true, isEightHoursDiff(dateTime1, dateTime2))
    } catch (Exception e) {
        throw exception("Wrong datetime formatter: " + e.getMessage())
    }

    // set time_zone back
    sql """ SET time_zone = "Asia/Shanghai" """
}
