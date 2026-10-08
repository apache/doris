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

suite("test_pg_all_types_select", "p0,external") {
    // Zoned JDBC types preserve instants; pin their display zone independently of the runner.
    sql "SET time_zone = '+08:00'"

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");

        sql """drop catalog if exists pg_all_type_test """
        sql """create catalog if not exists pg_all_type_test properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver"
        );"""

        sql """use pg_all_type_test.catalog_pg_test"""

        qt_desc_all_types_null """desc catalog_pg_test.extreme_test;"""

        // Range conversion and local filtering must agree, including before a pushed LIMIT.
        order_qt_extreme_nulls "select timestamptz_val is null from catalog_pg_test.extreme_test"
        order_qt_extreme_null_ids "select id from catalog_pg_test.extreme_test where timestamptz_val is null"
        order_qt_extreme_non_null_ids "select id from catalog_pg_test.extreme_test where timestamptz_val is not null"
        order_qt_extreme_null_limit """select id from catalog_pg_test.extreme_test
                                     where timestamptz_val is null order by id limit 1"""

        // Keep the remote table after the run so failed range conversions can be inspected.
        def executeRangeDdl = { String statement ->
            sql("CALL EXECUTE_STMT('pg_all_type_test', '" + statement.replace("'", "''") + "')")
        }
        executeRangeDdl("DROP TABLE IF EXISTS catalog_pg_test.timestamp_range_nullability")
        executeRangeDdl("CREATE TABLE catalog_pg_test.timestamp_range_nullability " +
                "(id INT NOT NULL, event_time TIMESTAMPTZ NOT NULL, other_time TIMESTAMPTZ NOT NULL)")
        executeRangeDdl("INSERT INTO catalog_pg_test.timestamp_range_nullability VALUES " +
                "(1, '9999-12-31 23:59:59-08', '10000-01-02 00:00:00+00'), " +
                "(2, '2023-11-05 08:30:00+00', '2023-11-05 08:30:00+00'), " +
                "(3, 'infinity', '-infinity'), (4, '-infinity', 'infinity'), " +
                "(5, '0002-01-01 00:00:00+00 BC', '0002-01-02 00:00:00+00 BC'), " +
                "(6, '99999-01-01 00:00:00+00', '99999-01-02 00:00:00+00')")
        order_qt_range_nulls "select id, event_time is null from catalog_pg_test.timestamp_range_nullability"
        order_qt_range_null_ids """select id from catalog_pg_test.timestamp_range_nullability
                                  where event_time is null"""
        order_qt_range_non_null_ids """select id from catalog_pg_test.timestamp_range_nullability
                                      where event_time is not null"""
        order_qt_range_null_safe_equal """select id from catalog_pg_test.timestamp_range_nullability
                                         where event_time <=> other_time"""
        order_qt_range_equal """select id from catalog_pg_test.timestamp_range_nullability
                               where event_time = other_time"""
        order_qt_range_null_limit """select id from catalog_pg_test.timestamp_range_nullability
                                    where event_time is null order by id limit 1"""
        order_qt_range_count "select count(event_time) from catalog_pg_test.timestamp_range_nullability"

        qt_select_all_types_null """SELECT 
                                    id,
                                    smallint_val,
                                    int_val,
                                    bigint_val,
                                    decimal_val,
                                    real_val,
                                    double_val,
                                    char_val,
                                    LENGTH(varchar_val) AS varchar_val_length,
                                    LENGTH(text_val) AS text_val_length,
                                    date_val,
                                    timestamp_val,
                                    timestamptz_val,
                                    interval_val,
                                    bool_val,
                                    bytea_val,
                                    inet_val,
                                    cidr_val,
                                    macaddr_val,
                                    json_val,
                                    jsonb_val,
                                    point_val,
                                    line_val,
                                    circle_val,
                                    uuid_val
                                FROM 
                                    catalog_pg_test.extreme_test
                                ORDER BY 
                                    1;"""

        qt_select_all_types_multi_block """select count(*) from catalog_pg_test.extreme_test_multi_block;"""

        sql """drop catalog if exists pg_all_type_test """

        sql """drop catalog if exists pg_timestamp_tz_type_test """
        sql """create catalog if not exists pg_timestamp_tz_type_test properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=test_timestamp_tz_db&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver",
            "enable.mapping.timestamp_tz" = "true"
        );"""

        sql """SET time_zone = '+08:00';"""
        sql """use pg_timestamp_tz_type_test.test_timestamp_tz_db"""
        sql """ CALL EXECUTE_STMT("pg_timestamp_tz_type_test", "ALTER TABLE test_timestamp_tz_db.ts_test REPLICA IDENTITY FULL") """
        // Keep the write round trip repeatable without changing the preinstalled seed rows.
        sql """CALL EXECUTE_STMT('pg_timestamp_tz_type_test',
                'DELETE FROM test_timestamp_tz_db.ts_test WHERE id IN (3, 4)')"""
        qt_desc_timestamp_tz """desc ts_test;"""
        qt_select_timestamp_tz """select * from ts_test order by id;"""
        qt_select_timestamp_tz2 """insert into ts_test values(3,"1999-10-10 12:00:00+08:00","1999-10-10 12:00:00");"""
        qt_select_timestamp_tz3 """insert into ts_test values(4,NULL, NULL);"""
        qt_select_timestamp_tz5 """select * from ts_test order by id;"""
        sql """SET time_zone = '+00:00';"""
        qt_select_timestamp_tz6 """select * from ts_test order by id;"""
    }
}
