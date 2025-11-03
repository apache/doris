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

suite("test_datev1_common", "nonConcurrent") {

    sql """
        admin set frontend config("enable_date_conversion" = "false");
    """

    def table_normal = "test_datev1_common_normal_tbl"
    def table_dup = "test_datev1_common_dup_tbl" // duplicate key
    def table_uniq = "test_datev1_common_uniq_tbl" // unique key
    def table_agg = "test_datev1_common_agg_tbl" // aggregate key
    def table_dist = "test_datev1_common_dist_tbl" // distributed by
    def table_part = "test_datev1_common_part_tbl"; // partition by

    sql "drop table if exists ${table_dup}"
    sql "drop table if exists ${table_uniq}"
    sql "drop table if exists ${table_agg}"
    sql "drop table if exists ${table_dist}"
    sql "drop table if exists ${table_part}"

    sql """
        CREATE TABLE IF NOT EXISTS `${table_dup}` (
            `date_key1` datev1 NULL COMMENT "",
            `date_key2` datev1 NULL COMMENT "",
            `date_value1` datev1 NULL COMMENT "",
            `date_value2` datev1 NULL COMMENT "",
            INDEX `idx_key1` (`date_value1`) USING BITMAP,
            INDEX `idx_key2` (`date_value2`) USING BITMAP
          ) ENGINE=OLAP
          DUPLICATE KEY(`date_key1`, `date_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`date_key1`, `date_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${table_uniq}` (
            `date_key1` datev1 NULL COMMENT "",
            `date_key2` datev1 NULL COMMENT "",
            `date_value1` datev1 NULL COMMENT "",
            `date_value2` datev1 NULL COMMENT "",
            INDEX `idx_key1` (`date_value1`) USING BITMAP,
            INDEX `idx_key2` (`date_value2`) USING BITMAP
          ) ENGINE=OLAP
          UNIQUE KEY(`date_key1`, `date_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`date_key1`, `date_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${table_agg}` (
            `date_key1` datev1 NULL COMMENT "",
            `date_key2` datev1 NULL COMMENT "",
            `date_value1` datev1 replace NULL COMMENT "",
            `date_value2` datev1 replace NULL COMMENT ""
          ) ENGINE=OLAP
          AGGREGATE KEY(`date_key1`, `date_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`date_key1`, `date_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${table_dist}` (
            `date_key1` datev1 NULL COMMENT "",
            `date_key2` datev1 NULL COMMENT "",
            `date_value1` datev1 NULL COMMENT "",
            `date_value2` datev1 NULL COMMENT "",
            INDEX `idx_key1` (`date_value1`) USING BITMAP,
            INDEX `idx_key2` (`date_value2`) USING BITMAP
          ) ENGINE=OLAP
          DUPLICATE KEY(`date_key1`, `date_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`date_key1`, `date_key2`) BUCKETS 4
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS `${table_part}` (
            `date_key1` datev1 NULL COMMENT "",
            `date_key2` datev1 NULL COMMENT "",
            `date_value1` datev1 NULL COMMENT "",
            `date_value2` datev1 NULL COMMENT "",
            INDEX `idx_key1` (`date_value1`) USING BITMAP,
            INDEX `idx_key2` (`date_value2`) USING BITMAP
          ) ENGINE=OLAP
          UNIQUE KEY(`date_key1`, `date_key2`)
          PARTITION BY RANGE(`date_key1`) (
            PARTITION `p1` VALUES LESS THAN ('2010-12-30'),
            PARTITION `p2` VALUES LESS THAN ('2011-12-30'),
            PARTITION `p3` VALUES LESS THAN ('2012-12-30'),
            PARTITION `p4` VALUES LESS THAN ('2013-12-30'),
            PARTITION `p5` VALUES LESS THAN ('2014-12-30'),
            PARTITION `p6` VALUES LESS THAN ('2015-12-30')
          )
          DISTRIBUTED BY HASH(`date_key1`) BUCKETS 2
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
        )
    """

    def insert_data = { table_name ->
        sql """
            insert into ${table_name}
                values
                    ('2010-01-01', '2012-01-02', '2013-01-01', '2014-01-02'),
                    ('2010-01-02', '2012-02-02', '2013-02-02', '2014-03-04'),
                    ('2010-01-03', '2012-03-02', '2013-03-03', '2014-05-06'),
                    ('2010-01-04', '2012-04-02', '2013-04-04', '2014-07-08'),
                    ('2010-01-05', '2012-05-02', '2013-05-05', '2014-09-10'),
                    ('2010-01-06', '2012-06-02', '2013-06-06', '2014-11-12'),
                    ('2010-01-07', '2012-07-02', '2013-07-07', '2014-01-14'),
                    ('2010-01-08', '2012-08-02', '2013-08-08', '2014-02-16'),
                    ('2010-01-09', '2012-09-02', '2013-09-09', '2014-03-18');
        """
        sql """
            insert into ${table_name}
                values
                    ('2010-01-01', '2012-01-02', '2013-01-01', '2014-01-02'),
                    ('2010-01-02', '2012-02-02', '2013-02-02', '2014-03-04'),
                    ('2010-01-03', '2012-03-02', '2013-03-03', '2014-05-06'),
                    ('2010-01-04', '2012-04-02', '2013-04-04', '2014-07-08'),
                    ('2010-01-05', '2012-05-02', '2013-05-05', '2014-09-10'),
                    ('2010-01-06', '2012-06-02', '2013-06-06', '2014-11-12'),
                    ('2010-01-07', '2012-07-02', '2013-07-07', '2014-01-14'),
                    ('2010-01-08', '2012-08-02', '2013-08-08', '2014-02-16'),
                    ('2010-01-09', '2012-09-02', '2013-09-09', '2014-03-18');
        """
        sql """
            insert into ${table_name}
                values
                    ('2012-01-01', '2011-01-02', '2015-01-01', '2014-01-02'),
                    ('2013-01-02', '2012-02-02', '2015-02-02', '2014-03-04'),
                    ('2014-01-03', '2013-03-02', '2014-03-03', '2013-05-06'),
                    ('2015-01-04', '2014-04-02', '2014-04-04', '2013-07-08'),
                    ('2012-01-05', '2015-05-02', '2013-05-05', '2012-09-10'),
                    ('2013-01-06', '2014-06-02', '2013-06-06', '2012-11-12'),
                    ('2014-01-07', '2013-07-02', '2012-07-07', '2011-01-14'),
                    ('2015-01-08', '2012-08-02', '2012-08-08', '2011-02-16'),
                    ('2012-01-09', '2011-09-02', '2011-09-09', '2011-03-18');
        """
        sql """
            insert into ${table_name}
                values
                    (null, '2011-01-02', '2015-01-01', '2014-01-02'),
                    ('2013-01-02', null, '2015-02-02', '2014-03-04'),
                    ('2014-01-03', '2013-03-02', null, '2013-05-06'),
                    ('2015-01-04', '2014-04-02', '2014-04-04', null),
                    (null, null, '2013-05-05', '2012-09-10'),
                    ('2013-01-06', null, null, '2012-11-12'),
                    ('2014-01-07', '2013-07-02', null, null),
                    ('2015-01-08', null, null, null),
                    (null, null, null, '2011-03-18'),
                    (null, null, null, null);
        """
    }

    insert_data(table_dup)
    insert_data(table_agg)
    insert_data(table_uniq)
    insert_data(table_dist)
    insert_data(table_part)

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

    run_predicate_test(table_dup, "date_key1", "'2010-01-05'", "date_value1", "'2010-01-05'", "'2010-01-05', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");
    run_predicate_test(table_dup, "date_key2", "'2012-05-02'", "date_value2", "'2010-01-05'", "'2012-05-02', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");
    run_predicate_test(table_dup, "date_key1", "'2013-01-05'", "date_value1", "'2015-01-05'", "'2010-01-05', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");
    run_predicate_test(table_dup, "date_key2", "'2014-05-02'", "date_value2", "'2013-01-05'", "'2012-05-02', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");
    run_predicate_test(table_uniq, "date_key1", "'2010-01-05'", "date_value1", "'2010-01-05'", "'2010-01-05', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");
    run_predicate_test(table_agg, "date_key2", "'2012-05-02'", "date_value2", "'2010-01-05'", "'2012-05-02', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");
    run_predicate_test(table_dist, "date_key2", "'2012-05-02'", "date_value2", "'2010-01-05'", "'2012-05-02', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");
    run_predicate_test(table_part, "date_key2", "'2012-05-02'", "date_value2", "'2010-01-05'", "'2012-05-02', '2010-01-06', '2011-02-06', '2012-01-05', '2013-01-06'");

    def run_delete_test =  { table_name, where ->
        sql """
            delete from ${table_name} where ${where};
        """

        quickTest("after_del_${table_name}", """
            select * from ${table_name} order by 1, 2, 3, 4;
        """)
    }

    sql """set delete_without_partition=true; """
    sql "sync"
    run_delete_test(table_dup, "`date_value1` < '2013-01-05'")
    run_delete_test(table_uniq, "`date_value1` > '2012-06-02'")
    run_delete_test(table_agg, "`date_key1` = '2013-01-05'")
    run_delete_test(table_dist, "`date_value1` ='2012-06-02'")
    run_delete_test(table_part, "`date_value1` > '2012-06-02'")
    sql """set delete_without_partition=false; """
    sql """
        admin set frontend config("enable_date_conversion" = "true");
    """
}