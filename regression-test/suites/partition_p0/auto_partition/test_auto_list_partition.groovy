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

suite("test_auto_list_partition") {
    // varchar
    sql "drop table if exists list_table1"
    sql """
        CREATE TABLE `list_table1` (
            `str` varchar not null
        ) ENGINE=OLAP
        DUPLICATE KEY(`str`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`str`)
        (
        )
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into list_table1 values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc") """
    qt_sql1 """ select * from list_table1 order by `str` """
    def result11 = sql "show partitions from list_table1"
    assertEquals(result11.size(), 4)
    sql """ insert into list_table1 values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc"), ("new") """
    qt_sql2 """ select * from list_table1 order by `str` """
    def result12 = sql "show partitions from list_table1"
    assertEquals(result12.size(), 5)

    // char
    sql "drop table if exists list_table2"
    sql """
        CREATE TABLE `list_table2` (
            `ch` char not null
        ) ENGINE=OLAP
        DUPLICATE KEY(`ch`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`ch`)
        (
        )
        DISTRIBUTED BY HASH(`ch`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into list_table2 values (" "), ("!"), ("?"), ("1"), ("_"), ("x") """
    qt_sql3 """ select * from list_table2 order by `ch` """
    def result21 = sql "show partitions from list_table2"
    assertEquals(result21.size(), 6)
    sql """ insert into list_table2 values (" "), ("!"), ("?"), ("1"), ("_"), ("x"), ("_"), ("y") """
    qt_sql4 """ select * from list_table2 order by `ch` """
    def result22 = sql "show partitions from list_table2"
    assertEquals(result22.size(), 7)

    // varchar upper/lower case
    def tblName3 = "list_table3"
    sql "drop table if exists ${tblName3}"
    sql """
        CREATE TABLE `${tblName3}` (
            `k1` INT,
            `k2` VARCHAR(50) not null,
            `k3` DATETIMEV2(6)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`k2`)
        (
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into ${tblName3} values (1, 'ABC', '2000-01-01 12:12:12.123456'), (2, 'AAA', '2000-01-01'), (3, 'aaa', '2000-01-01'), (3, 'AaA', '2000-01-01') """
    def result3 = sql "show partitions from ${tblName3}"
    logger.info("${result3}")
    assertEquals(result3.size(), 4)

    // int
    sql "drop table if exists list_table4"
    sql """
        CREATE TABLE `list_table4` (
            `k1` INT not null,
            `k2` VARCHAR(50),
            `k3` DATETIMEV2(6)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`k1`)
        (
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into list_table4 values (1, '2020-12-12 12:12:12', '2000-01-01 12:12:12.123456'), (2, '20201212 121212', '2000-01-01'), (3, '20201212121212', '2000-01-01'), (3, 'AaA', '2000-01-01') """
    def result4 = sql "show partitions from list_table4"
    logger.info("${result4}")
    assertEquals(result4.size(), 3)

    // dtv2(6)
    sql "drop table if exists list_table5"
    sql """
        CREATE TABLE `list_table5` (
            `k1` INT,
            `k2` VARCHAR(50),
            `k3` DATETIMEV2(6) not null
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`k3`)
        (
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into list_table5 values (1, 'ABC', '2000-01-01 12:12:12.123456'), (2, 'AAA', '2000-01-01'), (3, 'aaa', '2000-01-01'), (3, 'AaA', '2000-01-01') """
    def result5 = sql "show partitions from list_table5"
    logger.info("${result5}")
    assertEquals(result5.size(), 2)

    // largeint
    sql "drop table if exists test_largeint"
    sql """
    CREATE TABLE test_largeint (
	    k largeint not null
    )
    AUTO PARTITION BY LIST (`k`)
    (
    )
    DISTRIBUTED BY HASH(`k`) BUCKETS 16
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql " insert into test_largeint values (1), (-1), (170141183460469231731687303715884105727) "
    def result6 = sql "show partitions from test_largeint"
    logger.info("${result6}")
    assertEquals(result6.size(), 3)

    // bool
    sql "drop table if exists test_bool"
    sql """
    CREATE TABLE test_bool (
	    k boolean not null
    )
    AUTO PARTITION BY LIST (`k`)
    (
    )
    DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql " insert into test_bool values (true), (false)"
    def result7 = sql "show partitions from test_bool"
    logger.info("${result7}")
    assertEquals(result7.size(), 2)

    // bigint
    sql "drop table if exists test_bigint"
    sql """
    CREATE TABLE test_bigint (
	    k bigint not null
    )
    AUTO PARTITION BY LIST (`k`)
    (
    )
    DISTRIBUTED BY HASH(`k`) BUCKETS 16
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql " insert into test_bigint values (1), (-1)"
    def result8 = sql "show partitions from test_bigint"
    logger.info("${result8}")
    assertEquals(result8.size(), 2)

    // smallint
    sql "drop table if exists test_smallint"
    sql """
    CREATE TABLE test_smallint (
	    k smallint not null
    )
    AUTO PARTITION BY LIST (`k`)
    (
    )
    DISTRIBUTED BY HASH(`k`) BUCKETS 16
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql " insert into test_smallint values (1), (-1)"
    def result9 = sql "show partitions from test_smallint"
    logger.info("${result9}")
    assertEquals(result9.size(), 2)

    // tinyint
    sql "drop table if exists test_tinyint"
    sql """
    CREATE TABLE test_tinyint (
	    k tinyint not null
    )
    AUTO PARTITION BY LIST (`k`)
    (
    )
    DISTRIBUTED BY HASH(`k`) BUCKETS 16
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql " insert into test_tinyint values (1), (-1)"
    def result10 = sql "show partitions from test_tinyint"
    logger.info("${result10}")
    assertEquals(result10.size(), 2)
    sql "drop table if exists test_list_many_column"
    sql """
        CREATE TABLE test_list_many_column (
            id int not null,
            k largeint not null
        )
        AUTO PARTITION BY LIST (`id`, `k`)
        (
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql " insert into test_list_many_column values (1,1), (-1,-1);"
    sql " insert into test_list_many_column values (1,3), (-1,-7);"
    result11 = sql "show partitions from test_list_many_column"
    logger.info("${result11}")
    assertEquals(result11.size(), 4)

    sql "drop table if exists test_list_many_column2"
    sql """
        CREATE TABLE test_list_many_column2 (
            id int not null,
            k largeint not null,
            str varchar not null
        )
        AUTO PARTITION BY LIST (`id`, `k`, `str`)
        (
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ insert into test_list_many_column2 values (1,1,"asd"), (-1,-1,"vdf");"""
    sql """ insert into test_list_many_column2 values (2,2,"xxx"), (-3,-3,"qwe");"""
    result12 = sql "show partitions from test_list_many_column2"
    logger.info("${result12}")
    assertEquals(result12.size(), 4)

    sql "drop table if exists test_list_many_column3"
    sql """
        CREATE TABLE test_list_many_column3 (
            id int not null,
            k largeint not null,
            str varchar not null
        )
        AUTO PARTITION BY LIST (`id`, `k`, `str`)
        (
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ insert into test_list_many_column3 values (1,1,"asd"), (-1,-1,"vdf");"""
    sql """ insert into test_list_many_column3 values (2,2,"xxx"), (-3,-3,"qwe");"""
    sql """ insert into test_list_many_column3 values (5,5,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaA")"""
    result12 = sql "show partitions from test_list_many_column3"
    logger.info("${result12}")
    assertEquals(result12.size(), 5)

    sql "drop table if exists stream_load_list_test_table_string_key"
    sql """
        CREATE TABLE `stream_load_list_test_table_string_key`(
        `col1` bigint not null,
        `col2` varchar(16384) not null
        ) duplicate KEY(`col1`)
        AUTO PARTITION BY list(`col2`)
        (
        )
        DISTRIBUTED BY HASH(`col1`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """ insert into stream_load_list_test_table_string_key values (1,"20"), (2," ");"""
    sql """ insert into stream_load_list_test_table_string_key values (3,"!"), (4,"! ");"""
    sql """ insert into stream_load_list_test_table_string_key values (5,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaA")"""
    result12 = sql "show partitions from stream_load_list_test_table_string_key"
    logger.info("${result12}")
    assertEquals(result12.size(), 5)


}
