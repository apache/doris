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

suite("aggregate_groupby_constant") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """ DROP TABLE IF EXISTS table_500_undef_partitions2_keys3_properties4_distributed_by5; """
    sql """
            create table table_500_undef_partitions2_keys3_properties4_distributed_by5 (
            col_date_undef_signed date   ,
            pk int,
            col_int_undef_signed int   ,
            col_int_undef_signed2 int   ,
            col_date_undef_signed2 date   ,
            col_varchar_10__undef_signed varchar(10)   ,
            col_varchar_1024__undef_signed varchar(1024)   
            ) engine=olap
            DUPLICATE KEY(col_date_undef_signed, pk, col_int_undef_signed)
            PARTITION BY RANGE(col_date_undef_signed) ( FROM ('2023-12-09') TO ('2023-12-12') INTERVAL 1 DAY )
            distributed by hash(pk) buckets 10
            properties("replication_num" = "1");
        """
    sql """SELECT table1 . col_int_undef_signed2 AS field1,
                ( TO_DATE (CASE
                WHEN ( '2024-01-08' < '2024-02-18' ) THEN
                '2023-12-19'
                WHEN ( table1 . `col_date_undef_signed2` < DATE_ADD ( table1 . `col_date_undef_signed` , INTERVAL 6 DAY ) ) THEN
                '2026-02-18'
                ELSE DATE_ADD( table1 . `col_date_undef_signed` , INTERVAL 365 DAY ) END)) AS field2, MAX( DISTINCT table1 . `col_varchar_10__undef_signed` ) AS field3
            FROM table_500_undef_partitions2_keys3_properties4_distributed_by5 AS table1
            WHERE ( ( table1 . `col_date_undef_signed` is NOT NULL )
                    OR table1 . `col_int_undef_signed` <> NULL )
            GROUP BY  field1,field2
            ORDER BY  field1,field2 LIMIT 10000;"""
}
