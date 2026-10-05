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
suite("normalize_aggregate") {
    qt_test_upper_project_projections_rewrite """
        SELECT DISTINCT + + ( ( + + 46 ) ) * 89 AS col0, COUNT( * ) + + - 72 + - - 87 - AVG ( ALL - 56 ) * COUNT( * ) + - CASE + 49 WHEN 6 * + 76 + - +
        CAST( NULL AS SIGNED ) THEN NULL WHEN - COUNT( DISTINCT + + CAST( NULL AS SIGNED ) ) + 23 THEN NULL ELSE - + 43 * 32 - + 97 + - ( + 65 ) * + +
        CASE - 77 WHEN 5 THEN - 56 * + 26 ELSE NULL END / + COUNT( * ) + 20 + + 78 END * COALESCE ( COUNT( * ), - 60 - 90, + 42 * 27 - 98 * ( - 83 + 47 / 7 ),
        - ( NULLIF ( 61, 83 + 88 ) ) ) * 94; 
    """
    sql "drop table if exists normalize_aggregate_tab"
    sql """CREATE TABLE normalize_aggregate_tab(col0 INTEGER, col1 INTEGER, col2 INTEGER) distributed by hash(col0) buckets 10
        properties('replication_num' = '1'); """
    qt_test_upper_project_projections_rewrite2 """
    SELECT - + AVG ( DISTINCT - col0 ) * - col0 FROM
    normalize_aggregate_tab WHERE + - col0 IS NULL GROUP BY col0 HAVING NULL IS NULL;"""

    qt_test_lambda """
        select count(array_filter(i -> (i > 0.99), array(1, 2, 3)))
    """
}