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

suite("test_mysql_load", "p0") {
    sql "show tables"

    def tableName = "test_mysql_load_strict"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL,
            `v4` smallint(6) REPLACE_IF_NOT_NULL NULL,
            `v5` int(11) REPLACE_IF_NOT_NULL NULL,
            `v6` bigint(20) REPLACE_IF_NOT_NULL NULL,
            `v7` largeint(40) REPLACE_IF_NOT_NULL NULL,
            `v8` datetime REPLACE_IF_NOT_NULL NULL,
            `v9` date REPLACE_IF_NOT_NULL NULL,
            `v10` char(10) REPLACE_IF_NOT_NULL NULL,
            `v11` varchar(6) REPLACE_IF_NOT_NULL NULL,
            `v12` decimal(27, 9) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION partition_a VALUES [("-9223372036854775808"), ("100000")),
        PARTITION partition_b VALUES [("100000"), ("1000000000")),
        PARTITION partition_c VALUES [("1000000000"), ("10000000000")),
        PARTITION partition_d VALUES [("10000000000"), (MAXVALUE)))
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // strict mode success
    def test_strict_mode_file = getLoalFilePath "test_strict_mode.csv"

    sql """
        LOAD DATA 
        LOCAL
        INFILE '${test_strict_mode_file}'
        INTO TABLE ${tableName}
        PARTITION (partition_a, partition_b, partition_c, partition_d)
        COLUMNS TERMINATED BY '\t'
        (k1, k2, v2, v10, v11)
        PROPERTIES ("strict_mode"="true");
    """

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"


    // strict mode failed
    test {
        def test_strict_mode_fail_file = getLoalFilePath "test_strict_mode_fail.csv"

        sql """
            LOAD DATA 
            LOCAL
            INFILE '${test_strict_mode_fail_file}'
            INTO TABLE ${tableName}
            PARTITION (partition_a, partition_b, partition_c, partition_d)
            COLUMNS TERMINATED BY '\t'
            (k1, k2, v2, v10, v11)
            PROPERTIES ("strict_mode"="true");
        """
        exception "too many filtered rows"
    }

    // test_line_delimiter
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `id` int(11) NULL,
          `value` varchar(64) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    def test_line_delimiter_file = getLoalFilePath "test_line_delimiter.csv"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${test_line_delimiter_file}'
        INTO TABLE ${tableName}
        COLUMNS TERMINATED BY '|'
        LINES TERMINATED BY 'weizuo'
        (id, value);
    """

    sql "sync"
    def rowCount = sql "select count(1) from ${tableName}"
    assertEquals(3, rowCount[0][0])


    // test load_nullable_to_not_nullable
    def tableName2 = "load_nullable_to_not_nullable"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
    CREATE TABLE IF NOT EXISTS `${tableName2}` (
        k1 int(32) NOT NULL,
        k2 smallint NOT NULL,
        k3 int NOT NULL,
        k4 bigint NOT NULL,
        k5 decimal(9, 3) NOT NULL,
        k6 char(5) NOT NULL,
        k10 date NOT NULL,
        k11 datetime NOT NULL,
        k7 varchar(20) NOT NULL,
        k8 double max NOT NULL,
        k9 float sum NOT NULL )
    AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k7)
    PARTITION BY RANGE(k2) (
        PARTITION partition_a VALUES LESS THAN MAXVALUE
    )
    DISTRIBUTED BY HASH(k1, k2, k5)
    BUCKETS 3
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """

    def test_time_local = getLoalFilePath "test_time.data"
    sql """
        LOAD DATA 
        LOCAL
        INFILE '${test_time_local}'
        INTO TABLE ${tableName2}
        COLUMNS TERMINATED BY '\t'
        (col)
        SET (k1=year(col),k2=month(col),k3=month(col),k4=day(col),k5=7.7,k6="a",k10=date(col),k11=FROM_UNIXTIME(2019,"%Y-%m-%dT%H:%i:%s"),k7="k7",k8=month(col),k9=day(col));
    """
    
    sql "sync"
    order_qt_sql1 " SELECT * FROM ${tableName2}"

}

