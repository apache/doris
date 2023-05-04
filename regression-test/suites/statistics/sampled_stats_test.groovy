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

suite("test_sampled_stats") {
    def dbName = "test_sampled_stats"
    def tblName = "${dbName}.example_tbl"

    def colStatisticsTblName = "__internal_schema.column_statistics"
    def colHistogramTblName = "__internal_schema.histogram_statistics"

    def columnNames = """
        (
            `t_1682570060000_user_id`, `t_1682570060000_date`, 
            `t_1682570060000_city`, `t_1682570060000_age`, `t_1682570060000_sex`, 
            `t_1682570060000_last_visit_date`, `t_1682570060000_cost`, 
            `t_1682570060000_max_dwell_time`, `t_1682570060000_min_dwell_time`
        )
    """

    def columnNameValues = """
        (
            't_1682570060000_user_id', 't_1682570060000_date', 't_1682570060000_city', 
            't_1682570060000_age', 't_1682570060000_sex', 't_1682570060000_last_visit_date', 
            't_1682570060000_cost', 't_1682570060000_max_dwell_time', 't_1682570060000_min_dwell_time'
        ) 
    """

    def query_col_statistics_with_order_sql = """
        SELECT 
            count, 
            ndv, 
            null_count, 
            min, 
            max, 
            data_size_in_bytes 
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
            null_count, 
            data_size_in_bytes;
    """

    def query_col_histogram_with_order_sql = """
        SELECT 
            sample_rate, 
            buckets
        FROM 
            ${colHistogramTblName} 
        WHERE 
            col_id IN ${columnNameValues}
        ORDER BY 
            col_id,
            sample_rate, 
            buckets;
    """

    sql """
        DROP DATABASE IF EXISTS ${dbName};
    """

    sql """
        CREATE DATABASE IF NOT EXISTS ${dbName};
    """

    sql """
        DROP TABLE IF EXISTS ${tblName};
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${tblName} (
            `t_1682570060000_user_id` LARGEINT NOT NULL,
            `t_1682570060000_date` DATE NOT NULL,
            `t_1682570060000_city` VARCHAR(20),
            `t_1682570060000_age` SMALLINT,
            `t_1682570060000_sex` TINYINT,
            `t_1682570060000_last_visit_date` DATETIME REPLACE,
            `t_1682570060000_cost` BIGINT SUM,
            `t_1682570060000_max_dwell_time` INT MAX,
            `t_1682570060000_min_dwell_time` INT MIN
        ) ENGINE=OLAP
        AGGREGATE KEY(`t_1682570060000_user_id`, `t_1682570060000_date`,
         `t_1682570060000_city`, `t_1682570060000_age`, `t_1682570060000_sex`)
        PARTITION BY LIST(`t_1682570060000_date`)
        (
            PARTITION `p_201701` VALUES IN ("2017-10-01"),
            PARTITION `p_201702` VALUES IN ("2017-10-02"),
            PARTITION `p_201703` VALUES IN ("2017-10-03")
        )
        DISTRIBUTED BY HASH(`t_1682570060000_user_id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO ${tblName} ${columnNames}
        VALUES (10000, "2017-10-01", "Beijing", 20, 0, "2017-10-01 07:00:00", 15, 2, 2),
            (10001, "2017-10-01", "Beijing", 20, 0, "2017-10-01 06:00:00", 20, 10, 10),
            (10002, "2017-10-01", "Beijing", 30, 1, "2017-10-01 17:05:45", 2, 22, 22),
            (10003, "2017-10-02", "Shanghai", 20, 1, "2017-10-02 12:59:12", 200, 5, 5),
            (10004, "2017-10-02", "Shanghai", 20, 1, "2017-10-02 12:59:12", 200, 5, 5),
            (10005, "2017-10-02", "Guangzhou", 32, 0, "2017-10-02 11:20:00", 30, 11, 11),
            (10006, "2017-10-01", "Shenzhen", 35, 0, "2017-10-01 10:00:15", 100, 3, 3),
            (10007, "2017-10-03", "Shenzhen", 35, 0, "2017-10-03 10:20:22", 11, 6, 6),
            (10008, "2017-10-03", "Shenzhen", 35, 0, "2017-10-03 10:20:22", 11, 6, 6),
            (10009, "2017-10-03", "Shenzhen", 35, 0, "2017-10-03 10:20:22", 11, 6, 6);
    """

    sql """  
        DELETE FROM __internal_schema.column_statistics
        WHERE col_id IN (
            't_1682570060000_user_id', 't_1682570060000_date', 't_1682570060000_city', 
            't_1682570060000_age', 't_1682570060000_sex', 't_1682570060000_last_visit_date', 
            't_1682570060000_cost', 't_1682570060000_max_dwell_time', 't_1682570060000_min_dwell_time'
        );  
    """

    sql """  
        DELETE FROM __internal_schema.histogram_statistics
        WHERE col_id IN (
            't_1682570060000_user_id', 't_1682570060000_date', 't_1682570060000_city', 
            't_1682570060000_age', 't_1682570060000_sex', 't_1682570060000_last_visit_date', 
            't_1682570060000_cost', 't_1682570060000_max_dwell_time', 't_1682570060000_min_dwell_time'
        );  
    """

    sql """
        ANALYZE TABLE ${tblName} WITH sync;
    """

    sql """
        ANALYZE TABLE ${tblName} UPDATE HISTOGRAM WITH sync;
    """

    qt_sql query_col_statistics_with_order_sql

    qt_sql query_col_histogram_with_order_sql

    sql """
        ANALYZE TABLE ${tblName} WITH sync WITH SAMPLE ROWS 100;
    """

    sql """
        ANALYZE TABLE ${tblName} UPDATE HISTOGRAM WITH sync WITH SAMPLE ROWS 100;
    """

    qt_sql query_col_statistics_with_order_sql

    qt_sql query_col_histogram_with_order_sql

    sql """
        ANALYZE TABLE ${tblName} WITH sync WITH SAMPLE PERCENT 100;
    """

    sql """
        ANALYZE TABLE ${tblName} UPDATE HISTOGRAM WITH sync WITH SAMPLE PERCENT 100;
    """

    qt_sql query_col_statistics_with_order_sql

    qt_sql query_col_histogram_with_order_sql

    sql """
        ANALYZE TABLE ${tblName} WITH sync WITH SAMPLE ROWS 3;
    """

    sql """
        ANALYZE TABLE ${tblName} UPDATE HISTOGRAM WITH sync WITH SAMPLE ROWS 1;
    """

     // TODO Optimize the calculation method of the sample rate of the number of sampling rows
     // qt_sql """
     //     SELECT
     //         sample_rate
     //     FROM
     //         ${colHistogramTblName}
     //     WHERE
     //         col_id IN ${columnNameValues}
     //     ORDER BY
     //         col_id,
     //         sample_rate
     // """

    sql """
        ANALYZE TABLE ${tblName} WITH sync WITH SAMPLE PERCENT 50;
    """

    sql """
        ANALYZE TABLE ${tblName} UPDATE HISTOGRAM WITH sync WITH SAMPLE PERCENT 50;
    """

    qt_sql """
        SELECT 
            sample_rate
        FROM 
            ${colHistogramTblName} 
        WHERE 
            col_id IN ${columnNameValues}
        ORDER BY 
            sample_rate
    """

    sql """  
        DELETE FROM __internal_schema.column_statistics
        WHERE col_id IN (
            't_1682570060000_user_id', 't_1682570060000_date', 't_1682570060000_city', 
            't_1682570060000_age', 't_1682570060000_sex', 't_1682570060000_last_visit_date', 
            't_1682570060000_cost', 't_1682570060000_max_dwell_time', 't_1682570060000_min_dwell_time'
        );  
    """

    sql """  
        DELETE FROM __internal_schema.histogram_statistics
        WHERE col_id IN (
            't_1682570060000_user_id', 't_1682570060000_date', 't_1682570060000_city', 
            't_1682570060000_age', 't_1682570060000_sex', 't_1682570060000_last_visit_date', 
            't_1682570060000_cost', 't_1682570060000_max_dwell_time', 't_1682570060000_min_dwell_time'
        );  
    """

    sql """
        DROP DATABASE IF EXISTS ${dbName};
    """
}
