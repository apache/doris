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

suite("test_datetimev1_compare", "nonConcurrent") {

    /// legacy date/datetime format need to run in non concurrent mode.
    sql """
        admin set frontend config("enable_date_conversion" = "false");
    """

    def table_dup = "test_datetimev1_compare_dup_tbl" // duplicate key

    sql "drop table if exists ${table_dup};"

    sql """
        CREATE TABLE IF NOT EXISTS `${table_dup}` (
            `date_key1` datetimev1 NULL COMMENT "",
            `date_key2` datetimev1 NULL COMMENT ""
          ) ENGINE=OLAP
          DUPLICATE KEY(`date_key1`, `date_key2`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`date_key1`) BUCKETS 1
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
                    ('2010-01-01 19:29:39', '2012-01-02 19:29:39'),
                    ('2010-01-02 18:28:38', '2012-02-02 19:29:39'),
                    ('2010-01-03 23:37:47', '2012-03-02 19:29:39'),
                    ('2010-01-04 21:42:43', '2012-04-02'),
                    ('2010-01-05 17:58:59', '2012-05-02'),
                    ('2010-01-06 21:02:03', '2012-06-02 00:00:00'),
                    ('2010-01-07 09:29:39', '2012-07-02 00:00:00'),
                    ('2010-01-08 11:33:55', '2012-08-02 00:00:00'),
                    ('2010-01-09 14:43:42', '2012-09-02 00:00:00');
        """
    }

    insert_data(table_dup)

    def run_compare_test = { table_name, col, col_value ->
        def query1 = """
            select
                *
            from
                `${table_name}`
            where
                `${col}` < ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("compare_test_datetime_lt_${table_name}", query1)

        def query2 = """
            select
                *
            from
                `${table_name}`
            where
                `${col}` <= ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("compare_test_datetime_lte_${table_name}", query2)

        def query3 = """
            select
                *
            from
                `${table_name}`
            where
                date(`${col}`) < ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("compare_test_datetime_lt_${table_name}", query3)

        def query4 = """
            select
                *
            from
                `${table_name}`
            where
                date(`${col}`) <= ${col_value}
            order by 1, 2, 3, 4;
        """
        quickTest("compare_test_datetime_lte_${table_name}", query4)
    }

    run_compare_test(table_dup, "date_key1", "'2010-01-05 17:58:59'");
    run_compare_test(table_dup, "date_key2", "'2012-05-02 00:00:00'");
    run_compare_test(table_dup, "date_key1", "'2010-01-05 17:58:59'");
    run_compare_test(table_dup, "date_key2", "'2012-05-02 00:00:00'");

    sql """
        admin set frontend config("enable_date_conversion" = "true");
    """
}
