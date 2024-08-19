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

suite("test_schema_change_agg_check_all_types", "p0") {
    def tableName3 = "test_schema_change_agg_check_all_types"

    sql """ DROP TABLE IF EXISTS ${tableName3} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` int(30) sum NULL,
      `k5` largeint(40) sum NULL,
      `k6` float sum NULL,
      `k7` double sum NULL,
      `k8` decimal(9, 0) max NULL,
      `k9` char(10) replace NULL,
      `k10` varchar(1024) replace NULL,
      `k11` text replace NULL,
      `k12` date replace NULL,
      `k13` datetime replace NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(k1, k2, k3)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    // tinyint to smallint
    sql """ alter table ${tableName3} modify column k2 smallint key NULL"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }


    sql """ insert into ${tableName3} values (10001, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_tinyint_to_smallint """ select * from ${tableName3} """


    // smallint to int
    sql """ alter table ${tableName3} modify column k2 int key NULL"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10002, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_smallint_to_int """ select * from ${tableName3} """

    // int to bigint
    sql """ alter table ${tableName3} modify column k2 bigint key NULL"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10003, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_int_to_bigint """ select * from ${tableName3} """

    // bigint to largeint
    sql """ alter table ${tableName3} modify column k2 largeint key NULL"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10004, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_bigint_to_largeint """ select * from ${tableName3} """

    // largeint to float
    sql """ alter table ${tableName3} add column k14 largeint replace not null default "0" after k13"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ alter table ${tableName3} modify column k14 float replace not null default "0" after k13"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10005, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', 1.11) """

    qt_largeint_to_float """ select * from ${tableName3} """

    // float to double
    sql """ alter table ${tableName3} modify column k14 double replace not null default "0" after k13"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10006, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11) """

    qt_float_to_double """ select * from ${tableName3} """


    // float to decimal & string
    sql """ alter table ${tableName3} add column float1 float replace not null default "0" after k13"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }
    sql """ insert into ${tableName3} values (10007, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11,1.21) """

    qt_add_float""" select * from ${tableName3} """

    sql """ alter table ${tableName3} modify column float1 varchar replace not null default "0" after k13"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }
    sql """ insert into ${tableName3} values (10007, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11,'1.21') """
    qt_float_to_varchar""" select * from ${tableName3} """


    sql """ alter table ${tableName3} modify column float1 float replace not null default "0" after k13"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    qt_varchar_to_float""" select * from ${tableName3} """


    test {
        sql """ alter table ${tableName3} modify column float1 decimal(9, 2) replace not null default "0" after k13"""
        exception "Can not change FLOAT to DECIMAL32"
    }


     // add tinyint
    sql """ alter table ${tableName3} add column tinyint1 tinyint replace not null default "0" after k13"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }
    sql """ insert into ${tableName3} values (10007, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11,1.21,1) """

    qt_add_tinyint""" select * from ${tableName3} """

    //tinyint to int
    sql """ alter table ${tableName3} modify column tinyint1 int replace not null default "0" after k13"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10008, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11,1.21,1) """


    qt_tinyint_to_int""" select * from ${tableName3} """

    //int to bigint
    sql """ alter table ${tableName3} modify column tinyint1 bigint replace not null default "0" after k13"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10009, 2, 3, 4, 5, 6.6, 1.7, 8.8, 'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11,1.21,1) """

    qt_int_to_bigint""" select * from ${tableName3} """

    //bigint to largeint
    sql """ alter table ${tableName3} modify column tinyint1 largeint replace not null default "0" after k13"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10010, 2, 3, 4, 5, 6.6, 1.7, 8.8, 'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11,1.21,1) """

    qt_bigint_to_largeint""" select * from ${tableName3} """

    //largeint to double
    sql """ alter table ${tableName3} modify column tinyint1 double replace not null default "0" after k13"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ insert into ${tableName3} values (10011, 2, 3, 4, 5, 6.6, 1.7, 8.8, 'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11,1.21,1) """

    qt_largeint_to_double""" select * from ${tableName3} """



}

