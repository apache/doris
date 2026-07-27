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

suite("test_doris_jdbc_datetimev2_nano") {
    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}" +
            "/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    sql "switch internal"
    sql "create database if not exists regression_test_jdbc_datetimev2_nano"
    sql "use regression_test_jdbc_datetimev2_nano"
    sql "drop table if exists test_datetimev2_nano_external_source"
    sql "drop table if exists test_datetimev2_nano_external_imported"
    sql "drop table if exists test_datetimev2_nano_external_sink"
    sql """
        create table test_datetimev2_nano_external_source (
            id int,
            dt7 datetimev2(7),
            dt8 datetimev2(8),
            dt9 datetimev2(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table test_datetimev2_nano_external_imported (
            id int,
            dt7 datetimev2(7),
            dt8 datetimev2(8),
            dt9 datetimev2(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table test_datetimev2_nano_external_sink (
            id int,
            dt7 datetimev2(7),
            dt8 datetimev2(8),
            dt9 datetimev2(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_external_source values
        (1, '1677-09-21 00:12:43.1452242',
            '1677-09-21 00:12:43.14522420',
            '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.9999999',
            '1969-12-31 23:59:59.99999999',
            '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.0000000',
            '1970-01-01 00:00:00.00000000',
            '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.1234568',
            '1970-01-01 00:00:00.12345679',
            '1970-01-01 00:00:00.123456789'),
        (5, '2262-04-11 23:47:16.8547758',
            '2262-04-11 23:47:16.85477580',
            '2262-04-11 23:47:16.854775807'),
        (6, null, null, null),
        (7,
            convert_tz(
                cast('1970-01-01 08:00:00.0000000' as datetimev2(7)),
                'Asia/Shanghai', 'UTC'),
            convert_tz(
                cast('1970-01-01 08:00:00.00000000' as datetimev2(8)),
                'Asia/Shanghai', 'UTC'),
            convert_tz(
                cast('1970-01-01 08:00:00.000000000' as datetimev2(9)),
                'Asia/Shanghai', 'UTC'))
    """
    sql """
        insert into test_datetimev2_nano_external_source values
        (100, '1970-02-30 00:00:00.0000000',
              '1970-02-30 00:00:00.00000000',
              '1970-02-30 00:00:00.000000000')
    """
    sql """
        insert into test_datetimev2_nano_external_source values
        (101, '1677-09-21 00:12:43.1452241',
              '1677-09-21 00:12:43.14522419',
              '1677-09-21 00:12:43.145224191')
    """
    sql """
        insert into test_datetimev2_nano_external_source values
        (102, '2262-04-11 23:47:16.8547759',
              '2262-04-11 23:47:16.85477581',
              '2262-04-11 23:47:16.854775808')
    """

    sql "drop catalog if exists datetimev2_nano_doris_jdbc"
    sql """
        create catalog datetimev2_nano_doris_jdbc properties (
            "type" = "jdbc",
            "user" = "${jdbcUser}",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driverUrl}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        )
    """

    order_qt_read_datetimev2_nano_external_table """
        select id, dt7, dt8, dt9
        from datetimev2_nano_doris_jdbc.regression_test_jdbc_datetimev2_nano
             .test_datetimev2_nano_external_source
        order by id
    """
    order_qt_read_datetimev2_nano_external_predicate """
        select id, dt9
        from datetimev2_nano_doris_jdbc.regression_test_jdbc_datetimev2_nano
             .test_datetimev2_nano_external_source
        where dt9 in (
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.123456789' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9)))
        order by id
    """
    sql """
        insert into internal.regression_test_jdbc_datetimev2_nano
            .test_datetimev2_nano_external_imported
        select id, dt7, dt8, dt9
        from datetimev2_nano_doris_jdbc.regression_test_jdbc_datetimev2_nano
             .test_datetimev2_nano_external_source
    """
    order_qt_import_from_datetimev2_nano_external_table """
        select id, dt7, dt8, dt9
        from internal.regression_test_jdbc_datetimev2_nano
             .test_datetimev2_nano_external_imported
        order by id
    """

    sql """
        insert into datetimev2_nano_doris_jdbc.regression_test_jdbc_datetimev2_nano
            .test_datetimev2_nano_external_sink
        select id, dt7, dt8, dt9
        from internal.regression_test_jdbc_datetimev2_nano
             .test_datetimev2_nano_external_source
    """
    order_qt_write_datetimev2_nano_external_table """
        select id, dt7, dt8, dt9
        from internal.regression_test_jdbc_datetimev2_nano
             .test_datetimev2_nano_external_sink
        order by id
    """
}
