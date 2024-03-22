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

suite("test_outer_join", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def tbl1 = "test_outer_join1"
    def tbl2 = "test_outer_join2"
    def tbl3 = "test_outer_join3"

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl1} (
                c0 DECIMALV3(8,3)
            ) 
            DISTRIBUTED BY HASH (c0) BUCKETS 1 PROPERTIES ("replication_num" = "1");
        """

    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl2} (
                c0 CHAR(249)
            ) AGGREGATE KEY(c0) 
            DISTRIBUTED BY RANDOM BUCKETS 30 
            PROPERTIES ("replication_num" = "1");
        """

    sql "DROP TABLE IF EXISTS ${tbl3}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl3} (
                c0 DECIMALV3(8,3)
            ) 
            DISTRIBUTED BY HASH (c0) BUCKETS 1 PROPERTIES ("replication_num" = "1");
        """

    sql """INSERT INTO ${tbl2} (c0) VALUES ('dr'), ('x7Tq'), ('');"""
    sql """INSERT INTO ${tbl1} (c0) VALUES (0.47683432698249817), (0.8864791393280029);"""
    sql """INSERT INTO ${tbl1} (c0) VALUES (0.11287713050842285);"""
    sql """INSERT INTO ${tbl2} (c0) VALUES ('');"""
    sql """INSERT INTO ${tbl2} (c0) VALUES ('');"""
    sql """INSERT INTO ${tbl2} (c0) VALUES ('hb');"""

    qt_join """
            SELECT * FROM  ${tbl2} RIGHT  OUTER JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' LIKE 'abc') ORDER BY 2;    
    """
    qt_join """
            SELECT * FROM  ${tbl2} RIGHT  OUTER JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' NOT LIKE 'abc');
    """
    qt_join """
            SELECT * FROM  ${tbl2} JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' NOT LIKE 'abc');
    """
    qt_join """
            SELECT * FROM  ${tbl2} LEFT  OUTER JOIN ${tbl1} ON (('') like ('15DScmSM')) WHERE ('abc' NOT LIKE 'abc');    
    """

    sql "set disable_join_reorder=true"
    explain {
        sql "SELECT * FROM ${tbl1} RIGHT OUTER JOIN ${tbl3} ON ${tbl1}.c0 = ${tbl3}.c0"
        contains "RIGHT OUTER JOIN(PARTITIONED)"
    }
    explain {
        sql "SELECT * FROM ${tbl1} RIGHT ANTI JOIN ${tbl3} ON ${tbl1}.c0 = ${tbl3}.c0"
        contains "RIGHT ANTI JOIN(PARTITIONED)"
    }
    explain {
        sql "SELECT * FROM ${tbl1} FULL OUTER JOIN ${tbl3} ON ${tbl1}.c0 = ${tbl3}.c0"
        contains "FULL OUTER JOIN(PARTITIONED)"
    }

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql "DROP TABLE IF EXISTS ${tbl3}"

    sql """DROP TABLE IF EXISTS table_20_undef_partitions2_keys3_properties4_distributed_by511"""
    sql """DROP TABLE IF EXISTS table_20_undef_partitions2_keys3_properties4_distributed_by5"""
    sql """DROP TABLE IF EXISTS table_20_undef_partitions2_keys3_properties4_distributed_by52"""

    sql """create table table_20_undef_partitions2_keys3_properties4_distributed_by511 (
        pk int,
        col_int_undef_signed int   ,
        col_int_undef_signed2 int   
        ) engine=olap
        DUPLICATE KEY(pk, col_int_undef_signed)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");"""
    sql """create table table_20_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed int/*agg_type_placeholder*/   ,
        col_int_undef_signed2 int/*agg_type_placeholder*/   ,
        pk int/*agg_type_placeholder*/
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");"""
    sql """create table table_20_undef_partitions2_keys3_properties4_distributed_by52 (
        pk int,
        col_int_undef_signed int   ,
        col_int_undef_signed2 int   
        ) engine=olap
        DUPLICATE KEY(pk, col_int_undef_signed)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");"""

    sql """insert into table_20_undef_partitions2_keys3_properties4_distributed_by511(pk,col_int_undef_signed,col_int_undef_signed2) values (0,null,null),(1,-107,-2457422),(2,8286200,null),(3,18368,4386195),(4,-10561,7625),(5,null,4867223),(6,-31,28390),(7,null,null),(8,null,-7357378),(9,3603478,21),(10,4000423,118),(11,8153660,2),(12,null,-1066765),(13,null,-4828168),(14,-5083340,null),(15,85,-394),(16,3350274,-4843280),(17,-2460711,-67),(18,-24998,-113),(19,-72,-3137671);"""
    sql """insert into table_20_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2) values (0,6447625,-1027468),(1,null,null),(2,31378,null),(3,9,8081971),(4,-7503885,null),(5,null,-20883),(6,null,110),(7,12780,-22854),(8,null,121),(9,7121676,113),(10,-13201,29890),(11,null,-72),(12,8150635,null),(13,null,-49),(14,8319897,null),(15,3239704,null),(16,-8463,21764),(17,null,-17255),(18,7123047,-110),(19,null,10);"""
    sql """insert into table_20_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2) values (0,112,-15490),(1,973,-5913694),(2,null,-2259045),(3,null,null),(4,123,8378),(5,95,-16),(6,26099,-7768794),(7,31987,-2510),(8,null,28),(9,-50,-25696),(10,7367,37),(11,2707848,11007),(12,3,4023643),(13,-6478416,null),(14,4705,null),(15,-2399593,null),(16,-3102,4779393),(17,89,null),(18,-3296249,53),(19,26846,108);"""
    qt_select """SELECT T1.pk, T1.col_int_undef_signed2, T2 . col_int_undef_signed FROM table_20_undef_partitions2_keys3_properties4_distributed_by511 AS T1  
                LEFT JOIN  table_20_undef_partitions2_keys3_properties4_distributed_by5 AS T2 ON T1.col_int_undef_signed  >=  T2.col_int_undef_signed2  
                RIGHT JOIN  table_20_undef_partitions2_keys3_properties4_distributed_by52 AS T3 ON T2.col_int_undef_signed2  <=>  T3.col_int_undef_signed2 
                WHERE T3.pk  >=  1  AND  T1.pk  <>  5 order by 1, 2, 3;"""
}
