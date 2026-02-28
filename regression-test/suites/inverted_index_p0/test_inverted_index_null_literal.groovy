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

suite("test_inverted_index_null_literal") {

    def tableName = "test_inverted_index_null_literal"

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            pk INT,
            col_varchar VARCHAR(20),
            INDEX idx_col_varchar (`col_varchar`) USING INVERTED
        )
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES 
        (1, 'hello'),
        (2, 'world'),
        (3, NULL)
    """

    sql """ SET disable_nereids_expression_rules = "FOLD_CONSTANT_ON_FE" """
    sql """ SET enable_fold_constant_by_be = "true" """
    sql """ SET enable_sql_cache = 0 """

    qt_select_null_literal """
        SELECT pk FROM ${tableName} 
        WHERE col_varchar = NULL OR pk >= 0
        ORDER BY pk
    """

    qt_select_null_eq """SELECT pk FROM ${tableName} WHERE col_varchar = NULL ORDER BY pk"""
    qt_select_null_ne """SELECT pk FROM ${tableName} WHERE col_varchar != NULL ORDER BY pk"""
    qt_select_null_between """SELECT pk FROM ${tableName} WHERE col_varchar BETWEEN NULL AND NULL ORDER BY pk"""

    qt_select_is_null """SELECT pk FROM ${tableName} WHERE col_varchar IS NULL ORDER BY pk"""
    qt_select_is_not_null """SELECT pk FROM ${tableName} WHERE col_varchar IS NOT NULL ORDER BY pk"""

    sql "DROP TABLE IF EXISTS ${tableName}"
}
