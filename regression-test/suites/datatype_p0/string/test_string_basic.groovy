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

suite("test_string_basic") {
    sql "drop table if exists fail_tb1"
    // first column could not be string
    test {
        sql """CREATE TABLE IF NOT EXISTS fail_tb1 (k1 STRING NOT NULL, v1 STRING NOT NULL) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")"""
        exception "The olap table first column could not be float, double, string or array, struct, map, please use decimal or varchar instead."
    }
    // string type should could not be key
    test {
        sql """
            CREATE TABLE IF NOT EXISTS fail_tb1 ( k1 INT NOT NULL, k2 STRING NOT NULL)
            DUPLICATE KEY(k1,k2) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
            """
        exception "String Type should not be used in key column[k2]"
    }
    // create table if not exists with string column, insert and select ok
    def tbName = "str_tb"
    sql "drop table if exists ${tbName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbName} (k1 VARCHAR(10) NULL, v1 STRING NULL) 
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    // default repeat maximum is 10000
    sql """set repeat_max_num=131073"""
    sql """
        INSERT INTO ${tbName} VALUES
         ("", ""),
         (NULL, NULL),
         (1, repeat("test1111", 8192)),
         (2, repeat("test1111", 131072))
        """
    order_qt_select_str_tb "select k1, md5(v1), length(v1) from ${tbName}"

    sql """drop table if exists test_string_cmp;"""

    sql """
    CREATE TABLE `test_string_cmp` (
        `ts` datetime NULL,
        `s1` varchar(32) NULL,
        `s2` varchar(128) NULL
    ) ENGINE = OLAP
    DUPLICATE KEY(`ts`)
    DISTRIBUTED BY HASH(`s1`) BUCKETS 1
    PROPERTIES ( 
        "replication_num" = "1"
    );
    """

    sql """
    INSERT INTO `test_string_cmp` VALUES ('2023-02-22 12:00:00', '8001', '1'),
                                         ('2023-02-22 12:00:03', '8008', '8'),
                                         ('2023-02-22 12:00:03', '8008', '8008'),
                                         ('2023-02-22 12:00:03', null, '0');
    """

    qt_col_eq_col """
    select
        s1, s2,
        if(
            s1 = s2,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_neq_col """
    select
        s1, s2,
        if(
            s1 != s2,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_gt_col """
    select
        s1, s2,
        if(
            s1 > s2,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_eq_const """
    select
        s1, s2,
        if(
            s1 = '8008',
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_neq_const """
    select
        s1, s2,
        if(
            s1 != '8008',
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_lt_const """
    select
        s1, s2,
        if(
            s1 < '8008',
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_const_eq_col """
    select
        s1, s2,
        if(
            '8008' = s1,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_const_neq_col """
    select
        s1, s2,
        if(
            '8008' != s1,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_const_gt_col """
    select
        s1, s2,
        if(
            '8008' > s1,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_const_eq_const """
    select
        s1, s2,
        if(
            '8008' = substr('a8008', 2, 4),
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_const_neq_const """
    select
        s1, s2,
        if(
            '8008' != substr('a8008', 2, 4),
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_const_gt_const """
    select
        s1, s2,
        if(
            '8008' > substr('a8007', 2, 4),
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_eq_null """
    select
        s1, s2,
        if(
            s1 = null,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_neq_null """
    select
        s1, s2,
        if(
            s1 != null,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """

    qt_col_gt_null """
    select
        s1, s2,
        if(
            s1 > null,
            1,
            0
        ) as counts
    from
        test_string_cmp
    order by s1, s2, counts;
    """
}

