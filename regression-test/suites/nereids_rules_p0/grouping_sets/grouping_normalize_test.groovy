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

suite("grouping_normalize_test"){
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
         DROP TABLE IF EXISTS grouping_normalize_test
        """
    sql """
        CREATE TABLE `grouping_normalize_test` (
          `pk` INT NULL,
          `col_int_undef_signed` INT NULL,
          `col_int_undef_signed2` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`pk`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`pk`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql "insert into grouping_normalize_test values(1,3,5),(3,5,5),(31,2,5),(1,3,6),(3,6,2)"
    qt_test """
    SELECT  ROUND( SUM(pk  +  1)  -  3)  col_alias1,  MAX( DISTINCT  col_int_undef_signed  -  5)   AS col_alias2, pk  +  1  AS col_alias3
    FROM grouping_normalize_test  GROUP BY  GROUPING SETS ((col_int_undef_signed,col_int_undef_signed2,pk),()) order by 1,2,3;
    """
}