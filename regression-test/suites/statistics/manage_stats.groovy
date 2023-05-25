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

suite("test_manage_stats") {

    // TODO At present, "DELETE FROM" may fail to delete, so comment it out temporarily, wait for the community to fix
    // /**************************************** Constant definition Begin ****************************************/
    // def dbName = "test_manage_stats_db"
    // def tblName = "test_manage_stats_tbl"
    // def fullTblName = "${dbName}.${tblName}"
    //
    // def interDbName = "__internal_schema"
    // def tblStatisticsTblName = "${interDbName}.table_statistics"
    // def colHistogramTblName = "${interDbName}.histogram_statistics"
    // def colStatisticsTblName = "${interDbName}.column_statistics"
    // /***************************************** Constant definition End *****************************************/
    //
    //
    // /**************************************** Data initialization Begin ****************************************/
    // sql """
    //     DROP DATABASE IF EXISTS ${dbName};
    // """
    //
    // sql """
    //     CREATE DATABASE IF NOT EXISTS ${dbName};
    // """
    //
    // sql """
    //     DROP TABLE IF EXISTS ${fullTblName};
    // """
    //
    // sql """
    //     CREATE TABLE IF NOT EXISTS ${fullTblName} (
    //         `c_id` LARGEINT NOT NULL,
    //         `c_boolean` BOOLEAN,
    //         `c_int` INT,
    //         `c_float` FLOAT,
    //         `c_double` DOUBLE,
    //         `c_decimal` DECIMAL(6, 4),
    //         `c_varchar` VARCHAR(10),
    //         `c_datev2` DATEV2 NOT NULL
    //     ) ENGINE=OLAP
    //     DUPLICATE KEY(`c_id`)
    //     PARTITION BY LIST(`c_datev2`)
    //     (
    //         PARTITION `p_20230501` VALUES IN ("2023-05-01"),
    //         PARTITION `p_20230502` VALUES IN ("2023-05-02"),
    //         PARTITION `p_20230503` VALUES IN ("2023-05-03"),
    //         PARTITION `p_20230504` VALUES IN ("2023-05-04"),
    //         PARTITION `p_20230505` VALUES IN ("2023-05-05")
    //     )
    //     DISTRIBUTED BY HASH(`c_id`) BUCKETS 1
    //     PROPERTIES ("replication_num" = "1");
    // """
    // /***************************************** Data initialization End *****************************************/
    //
    // /************************************* Test1: Alter table stats Begin **************************************/
    // final int tblRowCount = 12345678
    //
    // sql """
    //     ALTER TABLE ${fullTblName} SET STATS ('row_count'='${tblRowCount}');
    // """
    //
    // qt_sql_check_alter_tbl_stats """
    //     SELECT `count` FROM ${tblStatisticsTblName} WHERE `count` = ${tblRowCount};
    // """
    // /************************************** Test1: Alter table stats End ***************************************/
    //
    //
    // /***************************************** Obtain table ID Begin ******************************************/
    // def tblIdRes = sql """
    //     SELECT `tbl_id` FROM ${tblStatisticsTblName} WHERE `count` = ${tblRowCount};
    // """
    //
    // assert (tblIdRes.size() == 1)
    // assert (tblIdRes[0].size() == 1)
    //
    // long tableId = tblIdRes[0][0] as long
    // /***************************************** Obtain table ID End *******************************************/
    //
    //
    // /************************************* Test2 Show table stats Begin **************************************/
    // sql """
    //     DELETE FROM ${tblStatisticsTblName} WHERE tbl_id = ${tableId};
    // """
    //
    // def tblCountSql = """
    //     SELECT COUNT(*) FROM ${tblStatisticsTblName} WHERE tbl_id = ${tableId};
    // """
    //
    // assertTrue(isSqlValueEqualToTarget(tblCountSql, "0", 10000, 30))
    //
    // sql """
    //     INSERT INTO ${tblStatisticsTblName} (id, catalog_id, db_id, tbl_id, idx_id, part_id, count,
    //       last_analyze_time_in_ms, update_time)
    //     VALUES (0, 0, 0, $tableId, -1, NULL, 10000, 1684655701003, "2023-06-23 22:30:29");
    // """
    //
    // qt_sql_check_show_tbl_stats """
    //     SHOW TABLE STATS ${fullTblName};
    // """
    // /*************************************** Test2 Show table stats End **************************************/
    //
    //
    // /************************************ Test3: Alter column stats Begin ************************************/
    // final String colName = "c_varchar"
    //
    // sql """
    //     ALTER TABLE ${fullTblName} MODIFY COLUMN `$colName`
    //     SET STATS (
    //         'row_count' = '87654321',
    //         'ndv' = '54321',
    //         'num_nulls' = '4321',
    //         'min_value' = 'abc',
    //         'max_value' = 'bcd',
    //         'data_size'='655360000'
    //     );
    // """
    //
    // qt_sql_check_alter_col_stats """
    //     SELECT `col_id`, `count`, `ndv`, `null_count`, `min`, `max`, `data_size_in_bytes`
    //     FROM ${colStatisticsTblName} WHERE tbl_id = ${tableId} AND `col_id` = "${colName}";
    // """
    // /************************************* Test3: Alter column stats End *************************************/
    //
    //
    // /************************************* Test4 Show column stats Begin *************************************/
    // sql """
    //     DELETE FROM ${colStatisticsTblName} WHERE tbl_id = ${tableId};
    // """
    //
    // def colCountSql = """
    //     SELECT COUNT(*) FROM ${colStatisticsTblName} WHERE tbl_id = ${tableId};
    // """
    //
    // assertTrue(isSqlValueEqualToTarget(colCountSql, "0", 10000, 30))
    //
    // sql """
    //     INSERT INTO ${colStatisticsTblName} (id, catalog_id, db_id, tbl_id, idx_id, col_id, part_id, count,
    //       ndv, null_count, min, max, data_size_in_bytes, update_time)
    //     VALUES (0, 0, 0, $tableId, -1, "${colName}", NULL, 10000, 1000, 100, "aaa", "sss", 1024, "2023-06-23 22:30:29");
    // """
    //
    // qt_sql_check_show_col_stats """
    //     SHOW COLUMN STATS ${fullTblName} ($colName);
    // """
    //
    // sql """
    //     INSERT INTO $colHistogramTblName (id, catalog_id, db_id, tbl_id, idx_id, col_id, sample_rate,
    //       buckets, update_time)
    //     VALUES (0, 0, 0, $tableId, -1, "${colName}", 0.8,
    //       "[{\'lower\':\'2022-09-21 17:30:29\',\'upper\':\'2022-09-21 22:30:29\',
    //       \'count\':9,\'pre_sum\':0,\'ndv\':1},'
    //       {\'lower\':\'2022-09-22 17:30:29\',\'upper\':\'2022-09-22 22:30:29\',
    //       \'count\':10,\'pre_sum\':9,\'ndv\':1},'
    //       {\'lower\':\'2022-09-23 17:30:29\',\'upper\':\'2022-09-23 22:30:29\',
    //       \'count\':9,\'pre_sum\':19,\'ndv\':1},'
    //       {\'lower\':\'2022-09-24 17:30:29\',\'upper\':\'2022-09-24 22:30:29\',
    //       \'count\':9,\'pre_sum\':28,\'ndv\':1},'
    //       {\'lower\':\'2022-09-25 17:30:29\',\'upper\':\'2022-09-25 22:30:29\',
    //       \'count\':9,\'pre_sum\':37,\'ndv\':1}]",
    //       "2023-06-23 22:30:29");
    // """
    //
    // qt_sql_check_show_col_histogram """
    //     SHOW COLUMN HISTOGRAM ${fullTblName} ($colName);
    // """
    // /*************************************** Test4 Show column stats End *************************************/
    //
    //
    // /**************************************** Test5 Drop stats Begin *****************************************/
    // sql """
    //     INSERT INTO ${colStatisticsTblName} (id, catalog_id, db_id, tbl_id, idx_id, col_id, part_id, count,
    //       ndv, null_count, min, max, data_size_in_bytes, update_time)
    //     VALUES (0, 0, 0, $tableId, -1, "${colName}", NULL, 10000, 1000, 100, "aaa", "sss", 1024, "2023-06-23 22:30:29");
    // """
    //
    // sql """
    //     INSERT INTO $colHistogramTblName (id, catalog_id, db_id, tbl_id, idx_id, col_id, sample_rate,
    //       buckets, update_time)
    //     VALUES (0, 0, 0, $tableId, -1, "${colName}", 0.8,
    //       "[{\'lower\':\'2022-09-21 17:30:29\',\'upper\':\'2022-09-21 22:30:29\',
    //       \'count\':9,\'pre_sum\':0,\'ndv\':1},'
    //       {\'lower\':\'2022-09-22 17:30:29\',\'upper\':\'2022-09-22 22:30:29\',
    //       \'count\':10,\'pre_sum\':9,\'ndv\':1},'
    //       {\'lower\':\'2022-09-23 17:30:29\',\'upper\':\'2022-09-23 22:30:29\',
    //       \'count\':9,\'pre_sum\':19,\'ndv\':1},'
    //       {\'lower\':\'2022-09-24 17:30:29\',\'upper\':\'2022-09-24 22:30:29\',
    //       \'count\':9,\'pre_sum\':28,\'ndv\':1},'
    //       {\'lower\':\'2022-09-25 17:30:29\',\'upper\':\'2022-09-25 22:30:29\',
    //       \'count\':9,\'pre_sum\':37,\'ndv\':1}]",
    //       "2023-06-23 22:30:29");
    // """
    //
    // sql """
    //     DROP STATS ${fullTblName} (${colName});
    // """
    //
    // def colStatCountSql = """
    //     SELECT COUNT(*) FROM $colStatisticsTblName WHERE tbl_id = ${tableId} AND col_id = "${colName}";
    // """
    //
    // def colHistCountSql = """
    //     SELECT COUNT(*) FROM $colHistogramTblName WHERE tbl_id = ${tableId} AND col_id = "${colName}";
    // """
    //
    // assertTrue(isSqlValueEqualToTarget(colStatCountSql, "0", 10000, 30))
    //
    // assertTrue(isSqlValueEqualToTarget(colHistCountSql, "0", 10000, 30))
    //
    // sql """
    //     INSERT INTO ${colStatisticsTblName} (id, catalog_id, db_id, tbl_id, idx_id, col_id, part_id, count,
    //       ndv, null_count, min, max, data_size_in_bytes, update_time)
    //     VALUES (0, 0, 0, $tableId, -1, "${colName}", NULL, 10000, 1000, 100, "aaa", "sss", 1024, "2023-06-23 22:30:29");
    // """
    //
    // sql """
    //     INSERT INTO $colHistogramTblName (id, catalog_id, db_id, tbl_id, idx_id, col_id, sample_rate,
    //       buckets, update_time)
    //     VALUES (0, 0, 0, $tableId, -1, "${colName}", 0.8,
    //       "[{\'lower\':\'2022-09-21 17:30:29\',\'upper\':\'2022-09-21 22:30:29\',
    //       \'count\':9,\'pre_sum\':0,\'ndv\':1},'
    //       {\'lower\':\'2022-09-22 17:30:29\',\'upper\':\'2022-09-22 22:30:29\',
    //       \'count\':10,\'pre_sum\':9,\'ndv\':1},'
    //       {\'lower\':\'2022-09-23 17:30:29\',\'upper\':\'2022-09-23 22:30:29\',
    //       \'count\':9,\'pre_sum\':19,\'ndv\':1},'
    //       {\'lower\':\'2022-09-24 17:30:29\',\'upper\':\'2022-09-24 22:30:29\',
    //       \'count\':9,\'pre_sum\':28,\'ndv\':1},'
    //       {\'lower\':\'2022-09-25 17:30:29\',\'upper\':\'2022-09-25 22:30:29\',
    //       \'count\':9,\'pre_sum\':37,\'ndv\':1}]",
    //       "2023-06-23 22:30:29");
    // """
    //
    // sql """
    //     DROP DATABASE IF EXISTS ${dbName};
    // """
    //
    // sql """
    //     DROP EXPIRED STATS;
    // """
    //
    // def statCountSql = """
    //     SELECT COUNT(*) FROM $colStatisticsTblName WHERE tbl_id = ${tableId};
    // """
    //
    // def histCountSql = """
    //     SELECT COUNT(*) FROM $colHistogramTblName WHERE tbl_id = ${tableId};
    // """
    //
    // assertTrue(isSqlValueEqualToTarget(statCountSql, "0", 10000, 30))
    //
    // // TODO drop expired histogram
    // sql """DELETE FROM $colHistogramTblName WHERE tbl_id = ${tableId}"""
    // // assertTrue(isSqlValueEqualToTarget(histCountSql, "0", 10000, 30))
    // /***************************************** Test5 Drop stats End *****************************************/
}

