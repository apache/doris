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

suite("test_decimalv2_common", "nonConcurrent") {

    sql """
        admin set frontend config("enable_decimal_conversion" = "false");
    """
    sql "set check_overflow_for_decimal=false;"

    def table_normal = "test_decimalv2_common_normal_tbl"
    def table_dup = "test_decimalv2_common_dup_tbl" // duplicate key
    def table_uniq = "test_decimalv2_common_uniq_tbl" // unique key
    def table_agg = "test_decimalv2_common_agg_tbl" // aggregate key
    def table_dist = "test_decimalv2_common_dist_tbl" // distributed by
    def table_part = "test_decimalv2_common_part_tbl"; // partition by

    sql "drop table if exists ${table_dup}"
    sql "drop table if exists ${table_uniq}"
    sql "drop table if exists ${table_agg}"
    sql "drop table if exists ${table_dist}"
    sql "drop table if exists ${table_part}"

    sql """
        CREATE TABLE IF NOT EXISTS `${table_dup}` (
            `decimal_key1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_key2` decimalv2(16, 5) NULL COMMENT "",
            `decimal_value1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_value2` decimalv2(16, 5) NULL COMMENT "",
            INDEX `idx_key1` (`decimal_value1`) USING BITMAP,
            INDEX `idx_key2` (`decimal_value2`) USING BITMAP
          ) ENGINE=OLAP
          DUPLICATE KEY(`decimal_key1`, `decimal_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`decimal_key1`, `decimal_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${table_uniq}` (
            `decimal_key1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_key2` decimalv2(16, 5) NULL COMMENT "",
            `decimal_value1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_value2` decimalv2(16, 5) NULL COMMENT "",
            INDEX `idx_key1` (`decimal_value1`) USING BITMAP,
            INDEX `idx_key2` (`decimal_value2`) USING BITMAP
          ) ENGINE=OLAP
          UNIQUE KEY(`decimal_key1`, `decimal_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`decimal_key1`, `decimal_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${table_agg}` (
            `decimal_key1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_key2` decimalv2(16, 5) NULL COMMENT "",
            `decimal_value1` decimalv2(8, 5) sum NULL COMMENT "",
            `decimal_value2` decimalv2(16, 5) max NULL COMMENT ""
          ) ENGINE=OLAP
          AGGREGATE KEY(`decimal_key1`, `decimal_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`decimal_key1`, `decimal_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${table_dist}` (
            `decimal_key1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_key2` decimalv2(16, 5) NULL COMMENT "",
            `decimal_value1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_value2` decimalv2(16, 5) NULL COMMENT "",
            INDEX `idx_key1` (`decimal_value1`) USING BITMAP,
            INDEX `idx_key2` (`decimal_value2`) USING BITMAP
          ) ENGINE=OLAP
          DUPLICATE KEY(`decimal_key1`, `decimal_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`decimal_key1`, `decimal_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    /*** TODO:  Column[decimal_key1] type[decimalv2] cannot be a range partition key.
    sql """
        CREATE TABLE IF NOT EXISTS `${table_part}` (
            `decimal_key1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_key2` decimalv2(16, 5) NULL COMMENT "",
            `decimal_value1` decimalv2(8, 5) NULL COMMENT "",
            `decimal_value2` decimalv2(16, 5) NULL COMMENT "",
            INDEX `idx_key1` (`decimal_value1`) USING BITMAP,
            INDEX `idx_key2` (`decimal_value2`) USING BITMAP
          ) ENGINE=OLAP
          UNIQUE KEY(`decimal_key1`, `decimal_key2`)
          PARTITION BY RANGE(`decimal_key1`) (
            PARTITION `p1` VALUES LESS THAN ('1.1111'),
            PARTITION `p2` VALUES LESS THAN ('2.2222'),
            PARTITION `p3` VALUES LESS THAN ('3.3333'),
            PARTITION `p4` VALUES LESS THAN ('4.4444'),
            PARTITION `p5` VALUES LESS THAN ('5.5555'),
            PARTITION `p6` VALUES LESS THAN ('100.9999')
          )
          DISTRIBUTED BY HASH(`decimal_key1`) BUCKETS 2
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """
    */

    def insert_data = { table_name ->
        sql """
            insert into ${table_name}
                values
                    (1.1111, 1.1111, 1.1111, 1.1111),
                    (2.2222, 1.1111, 1.1111, 1.1111),
                    (3.3333, 1.1111, 1.1111, 1.1111),
                    (4.4444, 1.1111, 1.1111, 1.1111),
                    (5.5555, 1.1111, 1.1111, 1.1111),
                    (6.6666, 1.1111, 1.1111, 1.1111),
                    (7.7777, 1.1111, 1.1111, 1.1111),
                    (8.8888, 1.1111, 1.1111, 1.1111),
                    (9.9999, 1.1111, 1.1111, 1.1111);
        """
        sql """
            insert into ${table_name}
                values
                    (1.1111, 1.1111, 1.1111, 1.1111),
                    (2.2222, 2.2222, 1.1111, 1.1111),
                    (3.3333, 3.3333, 1.1111, 1.1111),
                    (4.4444, 4.4444, 1.1111, 1.1111),
                    (5.5555, 5.5555, 1.1111, 1.1111),
                    (6.6666, 6.6666, 1.1111, 1.1111),
                    (7.7777, 7.7777, 1.1111, 1.1111),
                    (8.8888, 8.8888, 1.1111, 1.1111),
                    (9.9999, 9.9999, 1.1111, 1.1111);
        """
        sql """
            insert into ${table_name}
                values
                    (1.1111, 1.1111, 1.1111, 1.1111),
                    (2.2222, 2.2222, 2.2222, 1.1111),
                    (3.3333, 3.3333, 3.3333, 1.1111),
                    (4.4444, 4.4444, 4.4444, 1.1111),
                    (5.5555, 5.5555, 5.5555, 1.1111),
                    (6.6666, 6.6666, 6.6666, 1.1111),
                    (7.7777, 7.7777, 7.7777, 1.1111),
                    (8.8888, 8.8888, 8.8888, 1.1111),
                    (9.9999, 9.9999, 9.9999, 1.1111);
        """
        sql """
            insert into ${table_name}
                values
                    (1.1111, 1.1111, 1.1111, 1.1111),
                    (2.2222, 2.2222, 2.2222, 2.2222),
                    (3.3333, 3.3333, 3.3333, 3.3333),
                    (4.4444, 4.4444, 4.4444, 4.4444),
                    (5.5555, 5.5555, 5.5555, 5.5555),
                    (6.6666, 6.6666, 6.6666, 6.6666),
                    (7.7777, 7.7777, 7.7777, 7.7777),
                    (8.8888, 8.8888, 8.8888, 8.8888),
                    (9.9999, 9.9999, 9.9999, 9.9999);
        """
        sql """
            insert into ${table_name}
                values
                    (null, 1.1111, 1.1111, 1.1111),
                    (2.2222, null, 2.2222, 2.2222),
                    (3.3333, 3.3333, null, 3.3333),
                    (4.4444, 4.4444, 4.4444, null),
                    (5.5555, 5.5555, 5.5555, null),
                    (6.6666, 6.6666, null, 6.6666),
                    (7.7777, null, 7.7777, 7.7777),
                    (null, 8.8888, 8.8888, 8.8888),
                    (9.9999, null, 9.9999, 9.9999);
        """
        sql """
            insert into ${table_name}
                values
                    (1.1111, null, 1.1111, 1.1111),
                    (2.2222, 2.2222, null, 2.2222),
                    (3.3333, 3.3333, 3.3333, null),
                    (null, 4.4444, 4.4444, 4.4444),
                    (5.5555, null, 5.5555, 5.5555),
                    (6.6666, 6.6666, null, 6.6666),
                    (7.7777, 7.7777, 7.7777, null),
                    (null, 8.8888, 8.8888, 8.8888),
                    (9.9999, null, 9.9999, 9.9999);
        """
    }

    insert_data(table_dup)
    insert_data(table_agg)
    insert_data(table_uniq)
    insert_data(table_dist)
    // insert_data(table_part)

    def run_predicate_test = { table_name, col, col_value, bitmap_col, bitmap_col_value, in_list ->
        def query = """
            select
                *
            from
                `${table_name}`
            where
                `${bitmap_col}` = ${bitmap_col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_bitmap_eq_${table_name}", query)
        def query2 = """
            select
                *
            from
                `${table_name}`
            where
                `${bitmap_col}` <> ${bitmap_col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_bitmap_ne_${table_name}", query2)

        def query3 = """
            select
                *
            from
                `${table_name}`
            where
                `${bitmap_col}` < ${bitmap_col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_bitmap_lt_${table_name}", query3)

        def query4 = """
            select
                *
            from
                `${table_name}`
            where
                `${bitmap_col}` > ${bitmap_col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_bitmap_gt_${table_name}", query4)

        query = """
            select
                *
            from
                `${table_name}`
            where
                `${col}` = ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_eq_${table_name}", query)
        query2 = """
            select
                *
            from
                `${table_name}`
            where
                `${col}` <> ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_ne_${table_name}", query2)

        query3 = """
            select
                *
            from
                `${table_name}`
            where
                `${col}` < ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_lt_${table_name}", query3)

        query4 = """
            select
                *
            from
                `${table_name}`
            where
                `${col}` > ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("predicate_test_gt_${table_name}", query4)

        quickTest("predicate_test_null_${table_name}", """
            select * from ${table_name} where ${col} is null order by 1, 2, 3, 4;
        """)

        quickTest("predicate_test_notnull_${table_name}", """
            select * from ${table_name} where ${col} is not null order by 1, 2, 3, 4;
        """)

        quickTest("predicate_test_in_${table_name}", """
            select * from ${table_name} where ${col} in (${in_list}) order by 1, 2, 3, 4;
        """)

        quickTest("predicate_test_notin_${table_name}", """
            select * from ${table_name} where ${col} not in (${in_list}) order by 1, 2, 3, 4;
        """)

        sql """ set runtime_filter_wait_infinitely = true; """

        sql " set runtime_filter_type = 1; "

        quickTest("runtime_filter_${table_name}_1", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)

        sql " set runtime_filter_type = 2; "

        quickTest("runtime_filter_${table_name}_2", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)

        sql " set runtime_filter_type = 4; "

        quickTest("runtime_filter_${table_name}_3", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)

        sql " set runtime_filter_type = 8; "

        quickTest("runtime_filter_${table_name}_4", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)

        sql """ set runtime_filter_wait_infinitely = 0; """

        sql " set runtime_filter_type = 1; "

        quickTest("runtime_filter_${table_name}_5", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)

        sql " set runtime_filter_type = 2; "

        quickTest("runtime_filter_${table_name}_6,", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)

        sql " set runtime_filter_type = 4; "

        quickTest("runtime_filter_${table_name}_7", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)

        sql " set runtime_filter_type = 8; "

        quickTest("runtime_filter_${table_name}_8", """
            select * from ${table_name} t1, ${table_name} t2 where t1.${col} = t2.${col} order by 1, 2, 3, 4, 5, 6, 7, 8;
        """)
    }

    run_predicate_test(table_dup, "decimal_key1", "5.0", "decimal_value1", "5.0", "4.4444, 5.5555, 6.6666, 7.7777");
    run_predicate_test(table_dup, "decimal_key2", "5.0", "decimal_value1", "5.0", "4.4444, 5.5555, 6.6666, 7.7777");
    run_predicate_test(table_dup, "decimal_value1", "5.0", "decimal_value1", "5.0", "4.4444, 5.5555, 6.6666, 7.7777");
    run_predicate_test(table_dup, "decimal_value2", "5.0", "decimal_value1", "5.0", "4.4444, 5.5555, 6.6666, 7.7777");
    run_predicate_test(table_uniq, 'decimal_key1', '5.5555', 'decimal_value1', '5.5555', "4.4444, 5.5555, 6.6666, 7.7777");
    run_predicate_test(table_agg, 'decimal_key1', '6.6666', 'decimal_value1', '6.6666', "4.4444, 5.5555, 6.6666, 7.7777");
    run_predicate_test(table_dist, 'decimal_key1', '7.7777', 'decimal_value1', 'cast(7.7777 as decimalv2(8, 5))', "4.4444, 5.5555, 6.6666, 7.7777");
    // run_predicate_test(table_part, 'decimal_key1', '8.88888', 'decimal_value1', '8.8888', "4.4444, 5.5555, 6.6666, 7.7777");

    def run_delete_test =  { table_name, where ->
        sql """
            delete from ${table_name} where ${where};
        """

        quickTest("after_del_${table_name}", """
            select * from ${table_name} order by 1, 2, 3, 4;
        """)
    }

    run_delete_test(table_dup, "`decimal_value1` < 5.0")
    run_delete_test(table_uniq, "`decimal_value1` > 9.9999")
    // delete predicate on value column only supports Unique table with merge-on-write enabled and Duplicate table
    // run_delete_test(table_agg, "`decimal_key1` = 7.77777")
    run_delete_test(table_dist, "`decimal_value1` = 8.8888")
    //run_delete_test(table_part, "`decimal_value1` > 10.9999")

    sql """
        admin set frontend config("enable_decimal_conversion" = "true");
    """
}