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

suite("test_incremental_stats") {
    def dbName = "test_incremental_stats"
    def tblName = "incremental_stats_tbl"
    def fullTblName = "${dbName}.${tblName}"

    def colStatisticsTblName = "__internal_schema.column_statistics"
    def analysisJobsTblName = "__internal_schema.analysis_jobs"

    def columnNames = """
        (
            `t_1682176142000_user_id`, `t_1682176142000_date`, 
            `t_1682176142000_city`, `t_1682176142000_age`, `t_1682176142000_sex`, 
            `t_1682176142000_last_visit_date`, `t_1682176142000_cost`, 
            `t_1682176142000_max_dwell_time`, `t_1682176142000_min_dwell_time`
        )
    """

    def columnNameValues = """
        (
            't_1682176142000_user_id', 't_1682176142000_date', 't_1682176142000_city', 
            't_1682176142000_age', 't_1682176142000_sex', 't_1682176142000_last_visit_date', 
            't_1682176142000_cost', 't_1682176142000_max_dwell_time', 't_1682176142000_min_dwell_time'
        ) 
    """

    def query_col_statistics_with_order_sql = """
        SELECT 
            col_id, min, max, count, ndv, null_count
        FROM 
            ${colStatisticsTblName} 
        WHERE 
            col_id IN ${columnNameValues}
        ORDER BY 
            col_id,
            min,
            max,
            count,
            ndv,
            null_count;
    """

    def query_analysis_jobs_count_sql = """
        SELECT 
            COUNT(*) 
        FROM 
            ${analysisJobsTblName} 
        WHERE 
            col_name IN ${columnNameValues};
    """

    sql """
        SET enable_save_statistics_sync_job = true;
    """

    sql """
        DROP DATABASE IF EXISTS ${dbName};
    """

    sql """
        CREATE DATABASE IF NOT EXISTS ${dbName};
    """

    sql """
        DROP TABLE IF EXISTS ${fullTblName};
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${fullTblName} (
            `t_1682176142000_user_id` LARGEINT NOT NULL,
            `t_1682176142000_date` DATEV2 NOT NULL,
            `t_1682176142000_city` VARCHAR(20),
            `t_1682176142000_age` SMALLINT,
            `t_1682176142000_sex` TINYINT,
            `t_1682176142000_last_visit_date` DATETIME REPLACE,
            `t_1682176142000_cost` BIGINT SUM,
            `t_1682176142000_max_dwell_time` INT MAX,
            `t_1682176142000_min_dwell_time` INT MIN
        ) ENGINE=OLAP
        AGGREGATE KEY(`t_1682176142000_user_id`, `t_1682176142000_date`,
         `t_1682176142000_city`, `t_1682176142000_age`, `t_1682176142000_sex`)
        PARTITION BY LIST(`t_1682176142000_date`)
        (
            PARTITION `p_201701` VALUES IN ("2017-10-01"),
            PARTITION `p_201702` VALUES IN ("2017-10-02"),
            PARTITION `p_201703` VALUES IN ("2017-10-03")
        )
        DISTRIBUTED BY HASH(`t_1682176142000_user_id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO ${fullTblName} ${columnNames}
        VALUES (10000, "2017-10-01", "Beijing", 20, 0, "2017-10-01 07:00:00", 15, 2, 2),
            (10000, "2017-10-01", "Beijing", 20, 0, "2017-10-01 06:00:00", 20, 10, 10),
            (10001, "2017-10-01", "Beijing", 30, 1, "2017-10-01 17:05:45", 2, 22, 22),
            (10002, "2017-10-02", "Shanghai", 20, 1, "2017-10-02 12:59:12", 200, 5, 5),
            (10003, "2017-10-02", "Guangzhou", 32, 0, "2017-10-02 11:20:00", 30, 11, 11),
            (10004, "2017-10-01", "Shenzhen", 35, 0, "2017-10-01 10:00:15", 100, 3, 3),
            (10004, "2017-10-03", "Shenzhen", 35, 0, "2017-10-03 10:20:22", 11, 6, 6);
    """

    // sql """
    //     DELETE FROM ${colStatisticsTblName}
    //     WHERE col_id IN ${columnNameValues};
    // """

    sql """
        DELETE FROM ${analysisJobsTblName}
        WHERE tbl_name = '${tblName}';
    """

    // 1. Firstly do a full collection of statistics
    sql """
        ANALYZE TABLE ${fullTblName} ${columnNames} WITH sync;
    """

    // Collecting all 9 columns will generate 9 tasks (total tasks: 9)
    qt_sql_1 query_analysis_jobs_count_sql

    // Check the collected statistics
    qt_sql_2 query_col_statistics_with_order_sql

    // Incrementally collect statistics
    sql """
        ANALYZE TABLE ${fullTblName} ${columnNames} WITH sync WITH incremental;
    """

    // The table data has not changed, and no new tasks should be generated (total tasks:  9)
    qt_sql_3 query_analysis_jobs_count_sql

    // Statistics won't change either
    qt_sql_4 query_col_statistics_with_order_sql

    // 2. Drop a partition, then re-collect statistics
    // --------------------------------------------------------------------
    result = sql """
                SELECT COUNT(*) FROM ${fullTblName};
             """
    int rowcount = result[0][0] as int

    pResult = sql """
                SELECT COUNT(*) FROM ${fullTblName} PARTITIONS(`p_201701`);
             """
    int pRowcount = pResult[0][0] as int

    sql """
        ALTER TABLE ${fullTblName} DROP PARTITION `p_201701`;
    """

    // Waiting to delete data
    int FailedCnt = 0
    do {
        newResult = sql """
                        SELECT COUNT(*) FROM ${fullTblName};
                     """
        int newRowcount = newResult[0][0] as int
        if (newRowcount = rowcount - pRowcount) break
        Thread.sleep(10000)
        FailedCnt ++
    } while (FailedCnt <= 30)

    if (FailedCnt > 30) {
        throw new IllegalStateException("Failed to delete partition data")
    }

    // Incrementally collect statistics
    sql """
        ANALYZE TABLE ${fullTblName} ${columnNames} WITH sync WITH incremental;
    """

    // Although the partition is deleted, there are no partition statistics to be collected,
    // but table statistics need to be updated, so 9 tasks will be generated, (total tasks: 9 + 9 = 18)
    qt_sql_5 query_analysis_jobs_count_sql

    // Statistics will change either (unstable, same as follow)
    // qt_sql_6 query_col_statistics_with_order_sql

    // 3. Add a partition, then re-collect statistics
    sql """
        ALTER TABLE ${fullTblName} ADD PARTITION `p_201701` VALUES IN ('2017-10-01');
    """

    sql """
        INSERT INTO ${fullTblName} ${columnNames}
        VALUES (10000, "2017-10-01", "Beijing", 20, 0, "2017-10-01 07:00:00", 15, 2, 2),
            (10000, "2017-10-01", "Beijing", 20, 0, "2017-10-01 06:00:00", 20, 10, 10),
            (10001, "2017-10-01", "Beijing", 30, 1, "2017-10-01 17:05:45", 2, 22, 22),
            (10004, "2017-10-01", "Shenzhen", 35, 0, "2017-10-01 10:00:15", 100, 3, 3);
    """

    // Incrementally collect statistics
    sql """
        ANALYZE TABLE ${fullTblName} ${columnNames} WITH sync WITH incremental;
    """

    // Adding a new partition will generate new tasks to incrementally collect
    // the corresponding partition information, so 9 tasks will be generated (total tasks: 18 + 9 = 27)
    qt_sql_7 query_analysis_jobs_count_sql

    // Statistics will change either
    // qt_sql_8 query_col_statistics_with_order_sql

    // 4. Add a new column, then re-collect statistics
    sql """
         ALTER TABLE ${fullTblName} ADD COLUMN t_1682176142000_new_column BIGINT SUM DEFAULT '0';
    """

    // Incrementally collect statistics
    sql """
        ANALYZE TABLE ${fullTblName} (
            `t_1682176142000_user_id`, `t_1682176142000_date`, 
            `t_1682176142000_city`, `t_1682176142000_age`, `t_1682176142000_sex`, 
            `t_1682176142000_last_visit_date`, `t_1682176142000_cost`, 
            `t_1682176142000_max_dwell_time`, `t_1682176142000_min_dwell_time`,
            `t_1682176142000_new_column`
        )
        WITH sync WITH incremental;
    """

    // Add a column, but the partition has not changed, so only the statistics of
    // the newly added column will be collected during incremental collection,
    // and one task will be generated (total tasks: 27 + 1 = 28)
    // qt_sql_9 """
    //     SELECT
    //         COUNT(*)
    //     FROM
    //         ${analysisJobsTblName}
    //     WHERE
    //         col_name IN (
    //             't_1682176142000_user_id', 't_1682176142000_date', 't_1682176142000_city',
    //             't_1682176142000_age', 't_1682176142000_sex', 't_1682176142000_last_visit_date',
    //             't_1682176142000_cost', 't_1682176142000_max_dwell_time', 't_1682176142000_min_dwell_time',
    //             't_1682176142000_new_column'
    //         );
    // """

    // Statistics will change either
    // qt_sql_10 query_col_statistics_with_order_sql

    // 5. Finally, collect statistics in full
    // sql """
    //     ANALYZE TABLE ${fullTblName} (
    //         `t_1682176142000_user_id`, `t_1682176142000_date`,
    //         `t_1682176142000_city`, `t_1682176142000_age`, `t_1682176142000_sex`,
    //         `t_1682176142000_last_visit_date`, `t_1682176142000_cost`,
    //         `t_1682176142000_max_dwell_time`, `t_1682176142000_min_dwell_time`,
    //         `t_1682176142000_new_column`
    //     ) WITH sync;
    // """

    // Full collection will recollect the statistics of all columns and update the statistics of the table.
    // So 10 tasks will be generated。 (total tasks: 28 + 10 = 38)
    // qt_sql_11 """
    //     SELECT
    //         COUNT(*)
    //     FROM
    //         ${analysisJobsTblName}
    //     WHERE
    //         col_name IN (
    //             't_1682176142000_user_id', 't_1682176142000_date', 't_1682176142000_city',
    //             't_1682176142000_age', 't_1682176142000_sex', 't_1682176142000_last_visit_date',
    //             't_1682176142000_cost', 't_1682176142000_max_dwell_time', 't_1682176142000_min_dwell_time',
    //             't_1682176142000_new_column'
    //         );
    // """

    // Compare statistics again
    // qt_sql_12 query_col_statistics_with_order_sql

    // Verify "DROP STATS"
    sql """
        DROP STATS ${fullTblName} (`t_1682176142000_user_id`);
    """

    // qt_sql_13 """
    //     SELECT * FROM ${colStatisticsTblName}
    //     WHERE col_id = 't_1682176142000_user_id';
    // """

    // Verify "DROP EXPIRED STATS"
    sql """
        DROP DATABASE IF EXISTS ${dbName};
    """

    sql """
        DROP EXPIRED STATS;
    """

    //  int rowCount = 0
    //  int failedCnt = 0

    //  do {
    //      result = sql """
    //                   SELECT COUNT(*) FROM ${colStatisticsTblName}
    //                   WHERE col_id IN ${columnNameValues};
    //               """
    //      rowCount = result[0][0] as int
    //      if (rowCount == 0) break
    //      Thread.sleep(10000)
    //      failedCnt++
    //  } while (failedCnt < 30)

    //  assertEquals(0, rowCount)

    // TODO Can it be deleted synchronously
    // qt_sql_14 """
    //     SELECT COUNT(*) FROM ${colStatisticsTblName}
    //     WHERE col_id IN ${columnNameValues};
    // """
}
