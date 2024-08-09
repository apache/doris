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

    explain {
            sql("SELECT col_int_undef_signed, col_int_undef_signed2, SUM(pk) FROM grouping_normalize_test GROUP BY GROUPING SETS ((col_int_undef_signed, col_int_undef_signed2));")
            notContains("VREPEAT_NODE")
    }

    explain {
            sql("SELECT col_int_undef_signed, col_int_undef_signed2, SUM(pk), grouping_id(col_int_undef_signed2) FROM grouping_normalize_test GROUP BY GROUPING SETS ((col_int_undef_signed, col_int_undef_signed2),());")
            contains("VREPEAT_NODE")
    }

    explain {
            sql("SELECT col_int_undef_signed, col_int_undef_signed2, SUM(pk) FROM grouping_normalize_test GROUP BY GROUPING SETS ((col_int_undef_signed, col_int_undef_signed2));")
            notContains("VREPEAT_NODE")
    }


    sql "drop table if exists normalize_repeat_name_unchanged"
    sql """create table normalize_repeat_name_unchanged (
            col_int_undef_signed int/*agg_type_placeholder*/    ,
                    col_int_undef_signed2 int/*agg_type_placeholder*/    ,
            col_float_undef_signed float/*agg_type_placeholder*/    ,
                    col_int_undef_signed3 int/*agg_type_placeholder*/    ,
            col_int_undef_signed4 int/*agg_type_placeholder*/    ,
                    col_int_undef_signed5 int/*agg_type_placeholder*/    ,
            pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");"""
    sql """
    insert into normalize_repeat_name_unchanged(pk,col_int_undef_signed,col_int_undef_signed2,col_float_undef_signed,
    col_int_undef_signed3,col_int_undef_signed4,col_int_undef_signed5) values (0,null,-27328,5595590,null,null,5767077),(1,3831,null,87,-14582,21,null),
    (2,10131,5907087,28248,2473748,88,-18315),(3,2352090,5694,5173440,null,null,-31126),(4,-26805,29932,null,-55,3148,-6705245),(5,null,null,41,57,-3060427,null),
    (6,118,25,3472000,-123,null,-2934940),(7,null,null,-109,112,-7344754,4326526),(8,null,-2169155,-19402,null,null,26943),(9,46,null,1736620,30084,13838,null),
    (10,24708,null,null,-806832,-116,676),(11,2232,-23025,null,9665,-27413,13457),(12,-6,-127,-5007917,20521,-48,2709),(13,-72,-127,3258,null,-6394361,-5580),
    (14,4494439,-1760025,-16580,66,6562396,-280256),(15,6099281,-73,-5376852,-303421,null,-1843),(16,122,-23380,null,7350221,111,null),
    (17,null,null,11356,null,11799,108),(18,-91,-88,39,-29582,null,121),(19,4991662,null,-220,7593505,-54,4086882);"""

    qt_test_name_unchange """
        SELECT
            col_int_undef_signed2 AS C1 ,
            col_int_undef_signed2
        FROM
        normalize_repeat_name_unchanged
        GROUP BY
        GROUPING SETS (
        (col_int_undef_signed2),
        (col_int_undef_signed2))
        order by 1,2
    """
}