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

suite("test_arithmetic_expressions") {

    def table1 = "test_arithmetic_expressions"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
      `k1` decimalv3(38, 18) NULL COMMENT "",
      `k2` decimalv3(38, 18) NULL COMMENT "",
      `k3` decimalv3(38, 18) NULL COMMENT ""
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values(1.1,1.2,1.3),
            (1.2,1.2,1.3),
            (1.5,1.2,1.3)
    """
    qt_select_all "select * from ${table1} order by k1"

    qt_select "select k1 * k2 from ${table1} order by k1"
    qt_select "select * from (select k1 * k2 from ${table1} union all select k3 from ${table1}) a order by 1"

    qt_select "select k1 * k2 * k3 from ${table1} order by k1"
    qt_select "select k1 * k2 * k3 * k1 * k2 * k3 from ${table1} order by k1"
    qt_select "select k1 * k2 / k3 * k1 * k2 * k3 from ${table1} order by k1"
    sql "drop table if exists ${table1}"

    sql """
        CREATE TABLE IF NOT EXISTS ${table1} (             `a` DECIMALV3(9, 3) NOT NULL, `b` DECIMALV3(9, 3) NOT NULL, `c` DECIMALV3(9, 3) NOT NULL, `d` DECIMALV3(9, 3) NOT NULL, `e` DECIMALV3(9, 3) NOT NULL, `f` DECIMALV3(9, 3) NOT
        NULL, `g` DECIMALV3(9, 3) NOT NULL , `h` DECIMALV3(9, 3) NOT NULL, `i` DECIMALV3(9, 3) NOT NULL, `j` DECIMALV3(9, 3) NOT NULL, `k` DECIMALV3(9, 3) NOT NULL)            DISTRIBUTED BY HASH(a) PROPERTIES("replication_num" = "1");
    """

    sql """
    insert into ${table1} values(999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999);
    """
    qt_select_all "select * from ${table1} order by a"

    qt_select "select a + b + c from ${table1};"
    qt_select "select (a + b + c) * d from ${table1};"
    qt_select "select (a + b + c) / d from ${table1};"
    qt_select "select a + b + c + d + e + f + g + h + i + j + k from ${table1};"
    sql "drop table if exists ${table1}"

    def table2 = "test_arithmetic_expressions"

    sql "drop table if exists ${table2}"
    sql """ create table ${table2} (
            id smallint,
            fz decimal(27,9),
            fzv3 decimalv3(27,9),
            fm decimalv3(38,10))
            DISTRIBUTED BY HASH(`id`) BUCKETS auto
            PROPERTIES
            (
                "replication_num" = "1"
            ); """

    sql """ insert into ${table2} values (1,92594283.129196000,92594283.129196000,147202.0000000000); """
    sql """ insert into ${table2} values (2,107684988.257976000,107684988.257976000,148981.0000000000); """
    sql """ insert into ${table2} values (3,76891560.464178000,76891560.464178000,106161.0000000000); """
    sql """ insert into ${table2} values (4,277170831.851350000,277170831.851350000,402344.0000000000); """

    qt_select """ select id, fz/fm as dec,fzv3/fm as decv3 from ${table2} ORDER BY id; """
    sql "drop table if exists ${table2}"
}
