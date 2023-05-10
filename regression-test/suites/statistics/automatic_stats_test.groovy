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

suite("test_automatic_stats") {
    def dbName = "test_automatic_stats"
    def tblName = "automatic_stats_tbl"
    def fullTblName = "${dbName}.${tblName}"

    def colStatisticsTblName = "__internal_schema.column_statistics"
    def colHistogramTblName = "__internal_schema.histogram_statistics"
    def analysisJobsTblName = "__internal_schema.analysis_jobs"

    def columnNames = """
        (
            `t_1683555707000_user_id`, `t_1683555707000_date`,
            `t_1683555707000_city`, `t_1683555707000_age`, `t_1683555707000_sex`,
            `t_1683555707000_last_visit_date`, `t_1683555707000_cost`,
            `t_1683555707000_max_dwell_time`, `t_1683555707000_min_dwell_time`
        )
    """

    def columnNameValues = """
        (
            't_1683555707000_user_id', 't_1683555707000_date', 't_1683555707000_city',
            't_1683555707000_age', 't_1683555707000_sex', 't_1683555707000_last_visit_date',
            't_1683555707000_cost', 't_1683555707000_max_dwell_time', 't_1683555707000_min_dwell_time'
        )
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
            `t_1683555707000_user_id` LARGEINT NOT NULL,
            `t_1683555707000_date` DATEV2 NOT NULL,
            `t_1683555707000_city` VARCHAR(20),
            `t_1683555707000_age` SMALLINT,
            `t_1683555707000_sex` TINYINT,
            `t_1683555707000_last_visit_date` DATETIME REPLACE,
            `t_1683555707000_cost` BIGINT SUM,
            `t_1683555707000_max_dwell_time` INT MAX,
            `t_1683555707000_min_dwell_time` INT MIN
        ) ENGINE=OLAP
        AGGREGATE KEY(`t_1683555707000_user_id`, `t_1683555707000_date`,
         `t_1683555707000_city`, `t_1683555707000_age`, `t_1683555707000_sex`)
        PARTITION BY LIST(`t_1683555707000_date`)
        (
            PARTITION `p_201701` VALUES IN ("2017-10-01"),
            PARTITION `p_201702` VALUES IN ("2017-10-02"),
            PARTITION `p_201703` VALUES IN ("2017-10-03")
        )
        DISTRIBUTED BY HASH(`t_1683555707000_user_id`) BUCKETS 1
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

    // sql """
    //     DELETE FROM ${analysisJobsTblName}
    //     WHERE tbl_name = '${tblName}';
    // """

    sql """
        SET enable_save_statistics_sync_job = true;
    """

    // Varify column stats
    sql """
        ANALYZE TABLE ${fullTblName} WITH sync WITH auto;
    """

    qt_sql_1 """
        SELECT
            tbl_name, col_name, analysis_type, analysis_mode,
            analysis_method, schedule_type, period_time_in_ms
        FROM
            ${analysisJobsTblName}
        WHERE
            tbl_name = '${tblName}'
        ORDER BY
            col_name;
    """

    sql """
        ALTER TABLE ${fullTblName} DROP PARTITION `p_201701`;
    """

    // Thread.sleep(180000)

    // sql_2 """
    //      SELECT
    //          tbl_name, col_name, analysis_type, analysis_mode, analysis_method,
    //          schedule_type, period_time_in_ms
    //      FROM
    //          ${analysisJobsTblName}
    //      WHERE
    //          tbl_name = '${tblName}'
    //      ORDER BY
    //          col_name;
    //  """

    // qt_sql_3 """
    //      SELECT
    //          col_id, min, max, count, ndv, null_count
    //      FROM
    //          ${colStatisticsTblName}
    //      WHERE
    //          col_id IN ${columnNameValues}
    //      ORDER BY
    //          col_id,
    //          min,
    //          max,
    //          count,
    //          ndv,
    //          null_count;
    //  """

    sql """
        SHOW TABLE STATS ${fullTblName};
    """

    sql """
        SHOW TABLE STATS ${fullTblName} PARTITION `p_201702`;
    """

    // Below test would failed on community pipeline for unknown reason, comment it temporarily
    // sql """
    //     DELETE FROM ${colStatisticsTblName}
    //     WHERE col_id IN ${columnNameValues};
    // """
    //
    // int colFailedCnt = 0
    // int colStatsCnt = 0
    //
    // do {
    //     result = sql """
    //                     SELECT COUNT(*) FROM ${colStatisticsTblName}
    //                     WHERE col_id IN ${columnNameValues};
    //                  """
    //     colStatsCnt = result[0][0] as int
    //     if (colStatsCnt > 0) break
    //     Thread.sleep(10000)
    //     colFailedCnt ++
    // } while (colFailedCnt < 30)
    //
    // assert(colStatsCnt > 0)

    // Varify Histogram stats
    // sql """
    //     DELETE FROM ${colHistogramTblName}
    //     WHERE col_id IN ${columnNameValues};
    // """

    // sql """
    //     ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM WITH sync WITH period 15;
    // """

    // Unstable, temporarily comment out, open after the reason is found out
    // qt_sql_4 """
    //     SELECT
    //         tbl_name, col_name, job_type, analysis_type, analysis_mode,
    //         analysis_method, schedule_type, period_time_in_ms
    //     FROM
    //         ${analysisJobsTblName}
    //     WHERE
    //         tbl_name = '${tblName}' AND analysis_type = 'HISTOGRAM'
    //     ORDER BY
    //         col_name;
    // """

    // Thread.sleep(1000 * 29)

    // qt_sql_5 """
    //     SELECT
    //         tbl_name, col_name, analysis_type, analysis_mode, analysis_method,
    //         schedule_type, period_time_in_ms
    //     FROM
    //         ${analysisJobsTblName}
    //     WHERE
    //         tbl_name = '${tblName}' AND analysis_type = 'HISTOGRAM'
    //     ORDER BY
    //         col_name;
    // """

    // qt_sql_6 """
    //     SELECT
    //         col_id,
    //         buckets
    //     FROM
    //         ${colHistogramTblName}
    //     WHERE
    //         col_id IN ${columnNameValues}
    //     ORDER BY
    //         col_id,
    //         buckets;
    // """

    // sql """
    //     DELETE FROM ${colHistogramTblName}
    //     WHERE col_id IN ${columnNameValues};
    // """

    // int histFailedCnt = 0
    // int histStatsCnt = 0

    // do {
    //     result = sql """
    //                     SELECT COUNT(*) FROM ${colHistogramTblName}
    //                     WHERE col_id IN ${columnNameValues};
    //                  """
    //     histStatsCnt = result[0][0] as int
    //     if (histStatsCnt > 0) break
    //     Thread.sleep(10000)
    //     histFailedCnt ++
    // } while (histFailedCnt < 30)

    // assert(histStatsCnt > 0)

    // sql """
    //     DROP DATABASE IF EXISTS ${dbName};
    // """

//    sql """
//        DELETE FROM ${analysisJobsTblName}
//        WHERE tbl_name = '${tblName}';
//     """
}
