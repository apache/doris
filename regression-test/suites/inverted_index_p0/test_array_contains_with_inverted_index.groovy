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

suite("test_array_contains_with_inverted_index"){
    // prepare test table
    def indexTblName = "tai"

    // If we use common expr pass to inverted index , we should set enable_common_expr_pushdown = true
    sql """ set enable_common_expr_pushdown = true; """
//    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
    sql """ set enable_pipeline_x_engine = true;"""
    sql """ set enable_profile = true;"""

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS `${indexTblName}` (
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

    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '6afef581285b6608bf80d5a4e46cf839', '[\"a\", \"b\", \"c\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', 'd93d942d985a8fb7547c72dada8d332d', '[\"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\", \"k\", \"l\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '48a33ec3453a28bce84b8f96fe161956', '[\"m\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '021603e7dcfe65d44af0efd0e5aee154', '[\"n\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '9fcb57ae675f0af4d613d9e6c0e8a2a2', '[\"o\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a3'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a4', NULL); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a5', '[]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a6', '[null,null,null]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a7', [null,null,null]); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a8', []); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', 'a648a447b8f71522f11632eba4b4adde', '[\"p\", \"q\", \"r\", \"s\", \"t\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', 'a9fb5c985c90bf05f3bee5ca3ae95260', '[\"u\", \"v\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', '0974e7a82e30d1af83205e474fadd0a2', '[\"w\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', '26823b3995ee38bd145ddd910b2f6300', '[\"x\"]'); """
    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES ('2019-01-01', 'ee27ee1da291e46403c408e220bed6e1', '[\"y\"]'); """
    sql """ set enable_common_expr_pushdown = true """

    qt_sql """ select count() from ${indexTblName}"""
    def param_contains = ["\'s\'", "\'\'", null]
    for (int i = 0 ; i < param_contains.size(); ++i) {
        def p = param_contains[i]
        log.info("param: ${p}")
        order_qt_sql """ select * from tai where array_contains(inventors, ${p}) order by id; """
        order_qt_sql """ select * from tai where array_contains(inventors, ${p}) and apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select * from tai where array_contains(inventors, ${p}) and apply_date = '2019-01-01' order by id; """
        order_qt_sql """ select * from tai where array_contains(inventors, ${p}) or apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select * from tai where !array_contains(inventors, ${p}) order by id; """
        order_qt_sql """ select * from tai where !array_contains(inventors, ${p}) and apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select * from tai where !array_contains(inventors, ${p}) and apply_date = '2019-01-01' order by id; """
        order_qt_sql """ select * from tai where !array_contains(inventors, ${p}) or apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select * from tai where (array_contains(inventors, ${p}) and apply_date = '2017-01-01') or apply_date = '2019-01-01' order by id; """
    }

    // test arrays_overlap with inverted index
    // now if we use inverted index we will not eval exprs
    def param = [["\'s\'", "\'t\'"], [], null, ["\'s\'", "\'\'", "\'t\'"], ["\'s\'", null, "\'t\'"], [null, "\'\'"], ["\'s\'", null, "\'t\'", "\'\'"]] // null for arrays_overlap will return null which in predicate will lead to return empty set
    for (int i = 0 ; i < param.size(); ++i) {
        def p = param[i]
        log.info("param: ${p}")
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where arrays_overlap(inventors, ${p}) order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where arrays_overlap(inventors, ${p}) order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where arrays_overlap(inventors, ${p}) and apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where arrays_overlap(inventors, ${p}) and apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where arrays_overlap(inventors, ${p}) and apply_date = '2019-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where arrays_overlap(inventors, ${p}) and apply_date = '2019-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where arrays_overlap(inventors, ${p}) or apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where arrays_overlap(inventors, ${p}) or apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where !arrays_overlap(inventors, ${p}) order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where !arrays_overlap(inventors, ${p}) order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where !arrays_overlap(inventors, ${p}) and apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where !arrays_overlap(inventors, ${p}) and apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where !arrays_overlap(inventors, ${p}) and apply_date = '2019-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where !arrays_overlap(inventors, ${p}) and apply_date = '2019-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where !arrays_overlap(inventors, ${p}) or apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where !arrays_overlap(inventors, ${p}) or apply_date = '2017-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = true)*/ * from tai where (arrays_overlap(inventors, ${p}) and apply_date = '2017-01-01') or apply_date = '2019-01-01' order by id; """
        order_qt_sql """ select /*+SET_VAR(enable_common_expr_pushdown = false)*/ * from tai where (arrays_overlap(inventors, ${p}) and apply_date = '2017-01-01') or apply_date = '2019-01-01' order by id; """
    }
}
