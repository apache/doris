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


suite("test_array_match_any", "p0"){
    // session param
    sql """ set enable_fallback_to_original_planner=false; """

    def indexTblName = "test_array_match_any"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE `${indexTblName}` (
      `apply_date` date NULL COMMENT '',
      `id` varchar(60) NOT NULL COMMENT '',
      `inventors` array<text> NULL COMMENT '',
      INDEX index_inverted_inventors(inventors) USING INVERTED  COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`apply_date`, `id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """

    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '6afef581285b6608bf80d5a4e46cf839', '[\"aa, bb\", \"bb cc\", \"c;d\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', 'd93d942d985a8fb7547c72dada8d332d', '[\"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\", \"k\", \"l\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '48a33ec3453a28bce84b8f96fe161956', '[\"m; n\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '021603e7dcfe65d44af0efd0e5aee154', '[\"n\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '9fcb57ae675f0af4d613d9e6c0e8a2a2', '[\"o\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a3'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a4', NULL); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a5', '[]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a6', '[null,null,null]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a7', [null,null,null]); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a8', []); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', 'a648a447b8f71522f11632eba4b4adde', '[\"p 1\", \"q 12\", \"r 34\", \"s 56\", \"t 78\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', 'ee27ee1da291e46403c408e220bed6e1', '[\"y\"]'); """

    // query with index
    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
    qt_sql_idx_1 """ select * from ${indexTblName} where array_match_any(inventors, "bb");"""
    qt_sql_idx_2 """ select * from ${indexTblName} where array_match_any(inventors, "bb,cc");"""
    qt_sql_idx_3 """ select * from ${indexTblName} where array_match_any(inventors, "d ");"""
    qt_sql_idx_4 """ select * from ${indexTblName} where array_match_any(inventors, "d aa");"""
    qt_sql_idx_5 """ select * from ${indexTblName} where array_match_any(inventors, "d,aa");"""

    // query without index
    sql """ set enable_common_expr_pushdown_for_inverted_index = false; """
    qt_sql_no_idx_1 """ select * from ${indexTblName} where array_match_any(inventors, "bb");"""
    qt_sql_no_idx_2 """ select * from ${indexTblName} where array_match_any(inventors, "bb,cc");"""
    qt_sql_no_idx_3 """ select * from ${indexTblName} where array_match_any(inventors, "d ");"""
    qt_sql_no_idx_4 """ select * from ${indexTblName} where array_match_any(inventors, "d aa");"""
    qt_sql_no_idx_5 """ select * from ${indexTblName} where array_match_any(inventors, "d,aa");"""

    // tokenized
    def indexTblName1 = "test_array_match_any1"

    sql "DROP TABLE IF EXISTS ${indexTblName1}"
    // create 1 replica table
    sql """
	CREATE TABLE `${indexTblName1}` (
          `apply_date` DATE NULL,
          `id` VARCHAR(60) NOT NULL,
          `inventors` ARRAY<TEXT> NULL,
          INDEX index_inverted_inventors (`inventors`) USING INVERTED PROPERTIES("char_filter_pattern" = ",", "parser" = "english", "lower_case" = "true", "char_filter_type" = "char_replace") COMMENT ''''''''
        ) ENGINE=OLAP
        DUPLICATE KEY(`apply_date`, `id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V1",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728"
        );
    """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '6afef581285b6608bf80d5a4e46cf839', '[\"aa, bb\", \"bb cc\", \"c;d\"]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', 'd93d942d985a8fb7547c72dada8d332d', '[\"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\", \"k\", \"l\"]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '48a33ec3453a28bce84b8f96fe161956', '[\"m; n\"]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '021603e7dcfe65d44af0efd0e5aee154', '[\"n\"]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '9fcb57ae675f0af4d613d9e6c0e8a2a2', '[\"o\"]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a3'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a4', NULL); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a5', '[]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a6', '[null,null,null]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a7', [null,null,null]); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a8', []); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', 'a648a447b8f71522f11632eba4b4adde', '[\"p 1\", \"q 12\", \"r 34\", \"s 56\", \"t 78\"]'); """
    sql """ INSERT INTO `${indexTblName1}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', 'ee27ee1da291e46403c408e220bed6e1', '[\"y\"]'); """

    // query with index
    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
    qt_sql_tokenized_idx_1 """ select * from ${indexTblName1} where array_match_any(inventors, "bb");"""
    qt_sql_tokenized_idx_2 """ select * from ${indexTblName1} where array_match_any(inventors, "bb,cc");"""
    qt_sql_tokenized_idx_3 """ select * from ${indexTblName1} where array_match_any(inventors, "d ");"""
    qt_sql_tokenized_idx_4 """ select * from ${indexTblName1} where array_match_any(inventors, "d aa");"""
    qt_sql_tokenized_idx_5 """ select * from ${indexTblName1} where array_match_any(inventors, "d,aa");"""

    // query without index
    sql """ set enable_common_expr_pushdown_for_inverted_index = false; """
    qt_sql_tokenized_no_idx_1 """ select * from ${indexTblName1} where array_match_any(inventors, "bb");"""
    qt_sql_tokenized_no_idx_2 """ select * from ${indexTblName1} where array_match_any(inventors, "bb,cc");"""
    qt_sql_tokenized_no_idx_3 """ select * from ${indexTblName1} where array_match_any(inventors, "d ");"""
    qt_sql_tokenized_no_idx_4 """ select * from ${indexTblName1} where array_match_any(inventors, "d aa");"""
    qt_sql_tokenized_no_idx_5 """ select * from ${indexTblName1} where array_match_any(inventors, "d,aa");"""

}