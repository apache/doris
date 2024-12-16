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

suite("test_bitmap_filter_nereids") {
    multi_sql """
        set runtime_filter_type = 16;
        DROP TABLE IF EXISTS bitmap_table_nereids;
    
        CREATE TABLE bitmap_table_nereids (
        `k1` int(11) NULL,
        `k2` bitmap BITMAP_UNION ,
        `k3` bitmap BITMAP_UNION 
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        insert into bitmap_table_nereids values 
        (1, bitmap_from_string('1, 3, 5, 7, 9, 11, 13, 99, 19910811, 20150402'),
            bitmap_from_string('32767, 1985, 255, 789, 1991')),
        (2, bitmap_from_string('10, 11, 12, 13, 14'), bitmap_empty());

        set enable_nereids_planner=true;
        set enable_fallback_to_original_planner=false;
        """
        
    qt_sql1 "select k1, k2 from test_query_db.bigtable where k1 in (select k2 from bitmap_table_nereids) order by k1;"

    qt_sql2 "select k1, k2 from test_query_db.bigtable where k1 + 1 in (select k2 from bitmap_table_nereids) order by k1;"

    qt_sql3 "select k1, k2 from test_query_db.bigtable where k1 not in (select k2 from bitmap_table_nereids where k1 = 1) order by k1;"

    qt_sql4 "select t1.k1, t1.k2 from test_query_db.bigtable t1 join test_query_db.baseall t3 on t1.k1 = t3.k1 where t1.k1 in (select k2 from bitmap_table_nereids where k1 = 1) order by t1.k1;"

    qt_sql5 "select k1, k2 from test_query_db.bigtable where k1 in (select k2 from bitmap_table_nereids) and k2 not in (select k3 from bitmap_table_nereids) order by k1;"

    qt_sql6 "select k2, count(k2) from test_query_db.bigtable where k1 in (select k2 from bitmap_table_nereids) group by k2 order by k2;"

    qt_sql7 "select k1, k2 from (select 2 k1, 2 k2) t where k1 in (select k2 from bitmap_table_nereids) order by 1, 2;"

    qt_sql8 "select k1, k2 from (select 11 k1, 11 k2) t where k1 in (select k2 from bitmap_table_nereids) order by 1, 2;"

    qt_sql9 "select k1, k2 from (select 2 k1, 11 k2) t where k1 not in (select k2 from bitmap_table_nereids) order by 1, 2;"

    qt_sql10 "select k1, k2 from (select 1 k1, 11 k2) t where k1 not in (select k2 from bitmap_table_nereids) order by 1, 2;"

    qt_sql11 "select k10 from test_query_db.bigtable where cast(k10 as bigint) in (select bitmap_or(k2, to_bitmap(20120314)) from bitmap_table_nereids b) order by 1;"

    qt_sql12 """
        with w1 as (select k1 from test_query_db.bigtable where k1 in (select k2 from bitmap_table_nereids)), w2 as (select k2 from test_query_db.bigtable where k2 in (select k3 from bitmap_table_nereids)) 
        select * from (select * from w1 union select * from w2) tmp order by 1;
    """

    qt_sql13 "select k1, k2 from test_query_db.bigtable where k1 in (select to_bitmap(10)) order by 1, 2"

    qt_sql14 "select k1, k2 from test_query_db.bigtable where k1 in (select bitmap_from_string('1,10')) order by 1, 2"

    test {
        sql "select k1, count(*) from test_query_db.bigtable b1 group by k1 having k1 in (select k2 from bitmap_table_nereids b2) order by k1;"
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function, and don't support filter"
    }

    sql "set ignore_storage_data_distribution=false"
    explain{
        sql "select k1, k2 from test_query_db.bigtable where k1 in (select k2 from bitmap_table_nereids) order by k1;"
        contains "RF000[bitmap]"
    }   

    explain{
        sql "select k1, k2 from test_query_db.bigtable where k1 not in (select k2 from bitmap_table_nereids where k1 = 1)"
        contains "RF000[bitmap]"
    }   

    explain{
        sql " select k1, k2 from (select 2 k1, 2 k2) t where k1 in (select k2 from bitmap_table_nereids)"
        notContains "RF000[bitmap]"
    }  
    sql "set parallel_pipeline_task_num=6;"
    qt_sql15 "select k1, k2 from test_query_db.bigtable where k1 in (select k2 from bitmap_table_nereids) order by k1;"

    //mark join
    qt_sq16 "select k1, k2 from test_query_db.bigtable where k1 in (select k2 from bitmap_table_nereids) or k1=2 order by k1"
}
