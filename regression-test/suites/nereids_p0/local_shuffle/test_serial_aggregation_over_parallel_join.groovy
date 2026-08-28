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

suite("test_serial_aggregation_over_parallel_join") {
    ["serial_agg_join_probe", "serial_agg_join_left", "serial_agg_join_right"].each { table ->
        sql "DROP TABLE IF EXISTS ${table}"
    }

    sql """CREATE TABLE serial_agg_join_probe (
                col_bigint BIGINT, col_v10 VARCHAR(10), col_v64 VARCHAR(64), pk INT
            ) ENGINE=OLAP DISTRIBUTED BY HASH(pk) BUCKETS 10
            PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE serial_agg_join_left (
                pk INT, col_bigint BIGINT, col_v10 VARCHAR(10), col_v64 VARCHAR(64)
            ) ENGINE=OLAP DUPLICATE KEY(pk, col_bigint, col_v10)
            DISTRIBUTED BY HASH(pk) BUCKETS 10 PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE serial_agg_join_right (
                pk INT, col_v10 VARCHAR(10), col_bigint BIGINT, col_v64 VARCHAR(64)
            ) ENGINE=OLAP DUPLICATE KEY(pk, col_v10)
            DISTRIBUTED BY HASH(pk) BUCKETS 10 PROPERTIES ("replication_num"="1")"""

    sql """INSERT INTO serial_agg_join_probe VALUES
            (-94,'had','y',0),(672609,'k','h',1),(-3766684,'a','p',2),(5070261,'on','x',3),
            (NULL,'u','at',4),(-86,'v','c',5),(21910,'how','m',6),(-63,'that''s','go',7),
            (-8276281,'s','a',8),(-101,'w','y',9)"""
    sql """INSERT INTO serial_agg_join_left VALUES
            (0,NULL,'g','i'),(1,-6138328,'z','do'),(2,-23217,'g','about'),(3,104,'you''re','z'),
            (4,NULL,'oh','i'),(5,-54,'want','to'),(6,NULL,'x','c'),(7,NULL,'you''re','come'),
            (8,3447,'really','from'),(9,-5459,'i','will')"""
    sql """INSERT INTO serial_agg_join_right VALUES
            (0,'right',NULL,'g'),(1,'on',-486256,'on'),(2,'I''ll',-1,'at'),(3,'h',29263,'don''t'),
            (4,'a',5453,'s'),(5,'j',-119,'can''t'),(6,'one',89,'n'),(7,'s',-7227,'u'),
            (8,'time',94,'b'),(9,'yes',1816630,'yes')"""

    def variables = "enable_local_shuffle_planner=true,enable_local_shuffle=true," +
            "enable_bucket_shuffle_join=true,ignore_storage_data_distribution=true," +
            "bucket_shuffle_downgrade_ratio=0.8,use_serial_exchange=false," +
            "parallel_pipeline_task_num=3,enable_sql_cache=false," +
            "enable_share_hash_table_for_broadcast_join=false"

    order_qt_count_distinct_left_join """SELECT /*+SET_VAR(${variables})*/ COUNT(DISTINCT t1.pk)
            FROM serial_agg_join_left t1 LEFT JOIN serial_agg_join_probe t2 ON t2.pk=t1.pk
            WHERE (t1.col_v64>'FVjnKolDTt' AND t1.col_v64<='z') OR t1.col_v64 IS NULL
               OR (t1.col_v10>'me' AND t1.col_v10<='zzzz' AND t1.col_bigint BETWEEN 3 AND 7)"""

    order_qt_sum_and_count_distinct_left_join """SELECT /*+SET_VAR(${variables})*/
            SUM(DISTINCT t1.pk), COUNT(DISTINCT t1.pk)
            FROM serial_agg_join_right t1 LEFT JOIN serial_agg_join_probe t2 ON t2.pk=t1.pk
            WHERE t1.pk IN (2,9) OR t1.col_bigint IN (1,8)
               OR (t1.col_v64>='MijtyYyxeA' AND t1.col_v64<'z'
                   AND t1.col_v64>='on' AND t1.col_v64<'zzzz')"""

    order_qt_sum_distinct_broad_predicate """SELECT /*+SET_VAR(${variables})*/ SUM(DISTINCT t1.pk)
            FROM serial_agg_join_right t1 LEFT JOIN serial_agg_join_probe t2 ON t2.pk=t1.pk
            WHERE (t1.col_v64>='QXQpaZhWfj' AND t1.col_v64<'z')
               OR (t1.col_v64>='fvPsFBZelL' AND t1.col_v64<='well')
               OR (t1.pk BETWEEN 0 AND 15 AND t1.col_v10 LIKE 'a%')
               OR (t1.pk>=3 AND t1.pk<4) OR t1.pk BETWEEN 0 AND 100 OR (t1.pk>7 AND t1.pk<=9)"""

    order_qt_sum_distinct_reversed_join """SELECT /*+SET_VAR(${variables})*/ SUM(DISTINCT t1.pk)
            FROM serial_agg_join_probe t1 LEFT JOIN serial_agg_join_right t2 ON t1.pk=t2.pk
            WHERE (t1.pk IS NOT NULL AND t1.pk IN (3,8,2,2)
                   AND t1.col_v64 IN ('didn''t','when','a','come','AgpEFIOTAN'))
               OR (t1.col_v64>'HoatMBMEwP' AND t1.col_v64<='zzzz') OR t1.pk BETWEEN 6 AND 11
               OR (t1.pk IS NULL AND t1.pk IN (5)) OR (t1.pk<=t1.col_bigint AND t1.pk IN (8))"""

    order_qt_sum_distinct_multi_outer_join """SELECT /*+SET_VAR(${variables})*/ SUM(DISTINCT t1.pk)
            FROM serial_agg_join_right t1 RIGHT OUTER JOIN serial_agg_join_probe t2 ON t2.pk=t2.pk
            LEFT JOIN serial_agg_join_left t3 ON t3.pk=t1.pk
            WHERE (t1.col_v10>'jHKKlhlHDn' AND t1.col_v10<'z'
                   AND t1.col_v10 NOT IN ('him','you''re'))
               OR (t1.col_v64>='j' AND t1.col_v64<='y')
               OR (t1.col_v10 NOT BETWEEN 'rxpMJWfBRX' AND 'z' AND t1.col_bigint IN (1000)
                   AND t1.col_bigint IS NULL AND t1.col_bigint BETWEEN 6 AND 15)"""

    def nativeVariables = "enable_local_shuffle_planner=false,enable_local_shuffle=true," +
            "parallel_pipeline_task_num=3,enable_sql_cache=false," +
            "enable_share_hash_table_for_broadcast_join=false"

    order_qt_native_private_broadcast_build """SELECT /*+SET_VAR(${nativeVariables})*/ SUM(DISTINCT t1.pk)
            FROM serial_agg_join_right t1 LEFT JOIN [broadcast] serial_agg_join_probe t2 ON t2.pk=t1.pk
            WHERE t1.pk BETWEEN 0 AND 9"""
}
