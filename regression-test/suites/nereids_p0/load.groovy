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

suite("load") {
    // init query case data
    def dbName = "nereids_test_query_db"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"
    sql """
        CREATE TABLE IF NOT EXISTS `baseall` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """
        CREATE TABLE IF NOT EXISTS `test` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace_if_not_null null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """
        CREATE TABLE IF NOT EXISTS `bigtable` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """
    streamLoad {
        table "baseall"
        db dbName
        set 'column_separator', ','
        file "baseall.txt"
    }
    sql "insert into ${dbName}.test select * from ${dbName}.baseall where k1 <= 3"
    sql "insert into ${dbName}.bigtable select * from ${dbName}.baseall"

    // table for compaction
    sql """
    CREATE TABLE IF NOT EXISTS compaction_tbl
    (
      user_id LARGEINT NOT NULL,
      date DATE NOT NULL,
      city VARCHAR(20),
      age SMALLINT,
      sex TINYINT,
      last_visit_date DATETIME REPLACE DEFAULT "1970-01-01 00:00:00",
      last_update_date DATETIME REPLACE_IF_NOT_NULL DEFAULT "1970-01-01 00:00:00",
      last_visit_date_not_null DATETIME REPLACE NOT NULL DEFAULT "1970-01-01 00:00:00",
      cost BIGINT SUM DEFAULT "0",
      max_dwell_time INT MAX DEFAULT "0",
      min_dwell_time INT MIN DEFAULT "99999",
      hll_col HLL HLL_UNION NOT NULL,
      bitmap_col Bitmap BITMAP_UNION NOT NULL
    ) AGGREGATE KEY(user_id, date, city, age, sex)
    DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");"""

    sql """insert into compaction_tbl values(123,"1999-10-10",'aaa',123,123,"1970-01-01 00:00:00","1970-01-01 00:00:00","1970-01-01 00:00:00",123,123,123,hll_hash(""),bitmap_from_string(""));"""

    def baseall_count = sql "select count(*) from ${dbName}.baseall"
    assertEquals(16, baseall_count[0][0])
    def test_count = sql "select count(*) from ${dbName}.test"
    assertEquals(3, test_count[0][0])

    sql"drop view if exists empty"
    sql"create view empty as select * from baseall where k1 = 0"
}

