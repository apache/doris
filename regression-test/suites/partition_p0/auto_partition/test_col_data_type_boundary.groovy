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

suite("test_col_data_type_boundary") {

    sql """drop table if exists table_bool"""
    sql """CREATE TABLE `table_bool` (
            `k1` bigint(20) not NULL,
            `c_bool` boolean not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_bool)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (c_bool) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_bool values (1, 1)"""
    sql """insert into table_bool values (2, 0)"""
    def partitions_res1 = sql """show partitions from table_bool order by PartitionId;"""
    sql """insert into table_bool values (3, true)"""
    sql """insert into table_bool values (4, false)"""
    sql """insert into table_bool values (4, 11)"""
    def select_rows = sql """select count() from table_bool;"""
    def partitions_res2 = sql """show partitions from table_bool order by PartitionId;"""
    assertEquals(select_rows[0][0], 5)
    assertEquals(partitions_res1.size(), 2)
    assertEquals(partitions_res2.size(), 2)
    assertEquals(partitions_res1[0][0], partitions_res2[0][0])
    assertEquals(partitions_res1[1][0], partitions_res2[1][0])

    sql """drop table if exists table_tinyint"""
    sql """CREATE TABLE `table_tinyint` (
            `k1` bigint(20) not NULL,
            `c_tinyint` tinyint(4) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_tinyint)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_tinyint) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_tinyint values(1, -128);"""
    sql """insert into table_tinyint values(1, -129);"""
    partitions_res1 = sql """show partitions from table_tinyint order by PartitionId;"""
    sql """insert into table_tinyint values(1, 127);"""
    sql """insert into table_tinyint values(2, 127);"""
    sql """insert into table_tinyint values(3, 128);"""
    partitions_res2 = sql """show partitions from table_tinyint order by PartitionId;"""
    select_rows = sql """select count() from table_tinyint;"""
    assertEquals(select_rows[0][0], 4)
    assertEquals(partitions_res1.size(), 2)
    assertEquals(partitions_res2.size(), 2)
    assertEquals(partitions_res1[0][0], partitions_res2[0][0])
    assertEquals(partitions_res1[1][0], partitions_res2[1][0])
    try {
        sql """alter table table_tinyint modify column c_tinyint largeint key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }

    sql """drop table if exists table_smallint"""
    sql """CREATE TABLE `table_smallint` (
            `k1` bigint(20) not NULL,
            `c_smallint` smallint(6) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_smallint)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_smallint) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_smallint values(1, -32768)"""
    sql """insert into table_smallint values(2, -32769)"""
    partitions_res1 = sql """show partitions from table_smallint order by PartitionId;"""
    sql """insert into table_smallint values(3, 32767)"""
    sql """insert into table_smallint values(4, 32768)"""
    partitions_res2 = sql """show partitions from table_smallint order by PartitionId;"""
    select_rows = sql """select count() from table_smallint;"""
    assertEquals(select_rows[0][0], 4)
    assertEquals(partitions_res1.size(), 2)
    assertEquals(partitions_res2.size(), 2)
    assertEquals(partitions_res1[0][0], partitions_res2[0][0])
    assertEquals(partitions_res1[1][0], partitions_res2[1][0])
    try {
        sql """alter table table_smallint modify column c_smallint largeint key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }

    sql """drop table if exists table_int"""
    sql """CREATE TABLE `table_int` (
            `k1` bigint(20) not NULL,
            `c_int` int(11) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_int)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_int) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_int values(1, -2147483648)"""
    sql """insert into table_int values(2, -2147483649)"""
    partitions_res1 = sql """show partitions from table_int order by PartitionId;"""
    sql """insert into table_int values(3, 2147483647)"""
    sql """insert into table_int values(4, 2147483648)"""
    partitions_res2 = sql """show partitions from table_int order by PartitionId;"""
    select_rows = sql """select count() from table_int;"""
    assertEquals(select_rows[0][0], 4)
    assertEquals(partitions_res1.size(), 2)
    assertEquals(partitions_res2.size(), 2)
    assertEquals(partitions_res1[0][0], partitions_res2[0][0])
    assertEquals(partitions_res1[1][0], partitions_res2[1][0])
    try {
        sql """alter table table_int modify column c_int largeint key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }

    sql """drop table if exists table_bigint"""
    sql """CREATE TABLE `table_bigint` (
            `k1` bigint(20) not NULL,
            `c_bigint` bigint(20) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_bigint)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_bigint) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_bigint values(1, -9223372036854775808)"""
    sql """insert into table_bigint values(2, -9223372036854775809)"""
    partitions_res1 = sql """show partitions from table_bigint order by PartitionId;"""
    sql """insert into table_bigint values(3, 9223372036854775807)"""
    sql """insert into table_bigint values(4, 9223372036854775808)"""
    partitions_res2 = sql """show partitions from table_bigint order by PartitionId;"""
    select_rows = sql """select count() from table_bigint;"""
    assertEquals(select_rows[0][0], 4)
    assertEquals(partitions_res1.size(), 2)
    assertEquals(partitions_res2.size(), 2)
    assertEquals(partitions_res1[0][0], partitions_res2[0][0])
    assertEquals(partitions_res1[1][0], partitions_res2[1][0])
    try {
        sql """alter table table_bigint modify column c_bigint largeint key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }

    sql """drop table if exists table_largeint"""
    sql """CREATE TABLE `table_largeint` (
            `k1` bigint(20) not NULL,
            `c_largeint` largeint not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_largeint)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_largeint) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""

    sql """drop table if exists tmp_varchar"""
    sql """CREATE TABLE `tmp_varchar` (
            `k1` bigint(20) not NULL,
            `c_varchar` varchar(65533) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_varchar)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_varchar) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into tmp_varchar values(1, "170141183460469231731687303715884105727")"""
    sql """insert into tmp_varchar values(2, "-170141183460469231731687303715884105728")"""
    sql """insert into tmp_varchar values(3, "170141183460469231731687303715884105728")"""
    sql """insert into tmp_varchar values(4, "-170141183460469231731687303715884105729")"""

    sql """insert into table_largeint select k1,c_varchar from tmp_varchar where k1=1;"""
    sql """insert into table_largeint select k1,c_varchar from tmp_varchar where k1=2;"""
    try {
        sql """insert into table_largeint select k1,c_varchar from tmp_varchar where k1=3;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Insert has filtered data in strict mode"))
    }
    try {
        sql """insert into table_largeint select k1,c_varchar from tmp_varchar where k1=4;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Insert has filtered data in strict mode"))
    }
    partitions_res1 = sql """show partitions from table_largeint order by PartitionId;"""
    select_rows = sql """select count() from table_largeint;"""
    assertEquals(select_rows[0][0], 2)
    assertEquals(partitions_res1.size(), 2)

    // float,double,decimal not support

    sql """drop table if exists table_date_range"""
    sql """CREATE TABLE `table_date_range` (
            `k1` bigint(20) not NULL,
            `c_date` date not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_date)
        COMMENT 'OLAP'
        AUTO PARTITION BY range (date_trunc(c_date, "day")) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_date_range values(1, "0000-01-01")"""
    sql """insert into table_date_range values(2, "9999-12-31")"""
    partitions_res1 = sql """show partitions from table_date_range order by PartitionId;"""
    select_rows = sql """select count() from table_date_range;"""
    assertEquals(select_rows[0][0], 2)
    assertEquals(partitions_res1.size(), 2)
    try {
        sql """alter table table_date_range modify column c_date datetime key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }


    sql """drop table if exists table_date_list"""
    sql """CREATE TABLE `table_date_list` (
            `k1` bigint(20) not NULL,
            `c_date` date not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_date)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_date) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_date_list values(1, "0000-01-01")"""
    sql """insert into table_date_list values(2, "9999-12-31")"""
    partitions_res1 = sql """show partitions from table_date_list order by PartitionId;"""
    select_rows = sql """select count() from table_date_list;"""
    assertEquals(select_rows[0][0], 2)
    assertEquals(partitions_res1.size(), 2)
    try {
        sql """alter table table_date_list modify column c_date datetime key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }

    sql """drop table if exists table_datetime_range"""
    sql """CREATE TABLE `table_datetime_range` (
            `k1` bigint(20) not NULL,
            `c_datetime` datetime(6) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_datetime)
        COMMENT 'OLAP'
        AUTO PARTITION BY range (date_trunc(c_datetime, "second")) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_datetime_range values(1, "0000-01-01")"""
    sql """insert into table_datetime_range values(1, "0000-01-01 00:00:00")"""
    sql """insert into table_datetime_range values(2, "9999-12-31 23:59:59")"""
    sql """insert into table_datetime_range values(2, "9999-12-31 23:59:59.999999")"""
    partitions_res1 = sql """show partitions from table_datetime_range order by PartitionId;"""
    select_rows = sql """select count() from table_datetime_range;"""
    assertEquals(select_rows[0][0], 3)
    assertEquals(partitions_res1.size(), 2)
    try {
        sql """alter table table_datetime_range modify column c_datetime date key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }

    sql """drop table if exists table_datetime_list"""
    sql """CREATE TABLE `table_datetime_list` (
            `k1` bigint(20) not NULL,
            `c_datetime` datetime(6) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_datetime)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_datetime) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_datetime_list values(1, "0000-01-01")"""
    sql """insert into table_datetime_list values(1, "0000-01-01 00:00:00")"""
    sql """insert into table_datetime_list values(2, "9999-12-31 23:59:59")"""
    sql """insert into table_datetime_list values(2, "9999-12-31 23:59:59.999999")"""
    partitions_res1 = sql """show partitions from table_datetime_list order by PartitionId;"""
    select_rows = sql """select count() from table_datetime_list;"""
    assertEquals(select_rows[0][0], 3)
    assertEquals(partitions_res1.size(), 3)
    try {
        sql """alter table table_datetime_list modify column c_datetime date key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }


    sql """drop table if exists table_char"""
    sql """CREATE TABLE `table_char` (
            `k1` bigint(20) not NULL,
            `c_char` char(255) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_char)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_char) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_char values(1, "")"""
    sql """insert into table_char values(2, "ddddddddddddddddddddddddddddddddddddddddddddddd")"""
    partitions_res1 = sql """show partitions from table_char order by PartitionId;"""
    select_rows = sql """select count() from table_char;"""
    assertEquals(select_rows[0][0], 2)
    assertEquals(partitions_res1.size(), 2)
    try {
        sql """alter table table_char modify column c_char varchar key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }


    sql """drop table if exists table_varchar"""
    sql """CREATE TABLE `table_varchar` (
            `k1` bigint(20) not NULL,
            `c_varchar` varchar(65533) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_varchar)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_varchar) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_varchar values(1, "")"""
    sql """insert into table_varchar values(2, "ddddddddddddddddddddddddddddddddddddddddddddddd")"""
    partitions_res1 = sql """show partitions from table_varchar order by PartitionId;"""
    select_rows = sql """select count() from table_varchar;"""
    assertEquals(select_rows[0][0], 2)
    assertEquals(partitions_res1.size(), 2)
    try {
        sql """alter table table_varchar modify column c_varchar date key;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify partition column"))
    }


    sql """drop table if exists table_datetime_range"""
    sql """CREATE TABLE `table_datetime_range` (
            `k1` bigint(20) not NULL,
            `c_datetime` datetime(6) not NULL,
            `c_int` int not null
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_datetime)
        COMMENT 'OLAP'
        AUTO PARTITION BY range (date_trunc(c_datetime, "second")) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");"""
    sql """insert into table_datetime_range values(1, "0000-01-01", 11)"""
    sql """insert into table_datetime_range values(1, "0000-01-01 00:00:00", 22)"""
    sql """insert into table_datetime_range values(2, "9999-12-31 23:59:59", 333)"""
    sql """insert into table_datetime_range values(2, "9999-12-31 23:59:59.999999", 444)"""
    partitions_res1 = sql """show partitions from table_datetime_range order by PartitionId;"""
    select_rows = sql """select count() from table_datetime_range;"""
    assertEquals(select_rows[0][0], 3)
    assertEquals(partitions_res1.size(), 2)

    sql """alter table table_datetime_range modify column c_int largeint;"""
    partitions_res1 = sql """show partitions from table_datetime_range order by PartitionId;"""
    select_rows = sql """select count() from table_datetime_range;"""
    assertEquals(select_rows[0][0], 3)
    assertEquals(partitions_res1.size(), 2)

}
