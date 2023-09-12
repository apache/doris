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
    def tblName1 = "list_table1"
    sql "drop table if exists ${tblName1}"
    sql """
        CREATE TABLE `${tblName1}` (
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
    sql """ insert into ${tblName1} values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc") """
    qt_sql1 """ select * from ${tblName1} order by `str` """
    result11 = sql "show partitions from ${tblName1}"
    assertEquals(result11.size(), 4)
    sql """ insert into ${tblName1} values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc"), ("new") """
    qt_sql2 """ select * from ${tblName1} order by `str` """
    result12 = sql "show partitions from ${tblName1}"
    assertEquals(result12.size(), 5)

    def tblName2 = "list_table2"
    sql "drop table if exists ${tblName2}"
    sql """
        CREATE TABLE `${tblName2}` (
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
    sql """ insert into ${tblName2} values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc") """
    qt_sql3 """ select * from ${tblName2} order by `str` """
    result21 = sql "show partitions from ${tblName2}"
    assertEquals(result21.size(), 4)
    sql """ insert into ${tblName2} values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc"), ("new") """
    qt_sql4 """ select * from ${tblName2} order by `str` """
    result22 = sql "show partitions from ${tblName2}"
    assertEquals(result22.size(), 5)

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
    result3 = sql "show partitions from ${tblName3}"
    logger.info("${result3}")
    assertEquals(result3.size(), 4)
}
