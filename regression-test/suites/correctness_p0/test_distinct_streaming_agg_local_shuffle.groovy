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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_distinct_streaming_agg_local_shuffle") {

    sql """drop table if exists table_10_undef_partitions2_keys3_properties4_distributed_by5;"""
    sql """ 
    create table table_10_undef_partitions2_keys3_properties4_distributed_by5 (
    col_bigint_undef_signed bigint/*agg_type_placeholder*/   ,
    col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
    col_varchar_64__undef_signed varchar(64)/*agg_type_placeholder*/   ,
    pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1"); """

    sql """
    insert into table_10_undef_partitions2_keys3_properties4_distributed_by5(pk,col_bigint_undef_signed,col_varchar_10__undef_signed,col_varchar_64__undef_signed) values (0,-94,'had','y'),(1,672609,'k','h'),(2,-3766684,'a','p'),(3,5070261,'on','x'),(4,null,'u','at'),(5,-86,'v','c'),(6,21910,'how','m'),(7,-63,'that''s','go'),(8,-8276281,'s','a'),(9,-101,'w','y');
    """

    sql """
    drop table if exists table_10_undef_partitions2_keys3_properties4_distributed_by52
    """

    sql """
    create table table_10_undef_partitions2_keys3_properties4_distributed_by52 (
    pk int,
    col_bigint_undef_signed bigint   ,
    col_varchar_10__undef_signed varchar(10)   ,
    col_varchar_64__undef_signed varchar(64)   
    ) engine=olap
    DUPLICATE KEY(pk, col_bigint_undef_signed, col_varchar_10__undef_signed)
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    """

    sql """
    insert into table_10_undef_partitions2_keys3_properties4_distributed_by52(pk,col_bigint_undef_signed,col_varchar_10__undef_signed,col_varchar_64__undef_signed) values (0,null,'g','i'),(1,-6138328,'z','do'),(2,-23217,'g','about'),(3,104,'you''re','z'),(4,null,'oh','i'),(5,-54,'want','to'),(6,null,'x','c'),(7,null,'you''re','come'),(8,3447,'really','from'),(9,-5459,'i','will');
    """

    sql """
    drop table if exists table_10_undef_partitions2_keys3_properties4_distributed_by53
    """

    sql """
    create table table_10_undef_partitions2_keys3_properties4_distributed_by53 (
    pk int,
    col_varchar_10__undef_signed varchar(10)   ,
    col_bigint_undef_signed bigint   ,
    col_varchar_64__undef_signed varchar(64)   
    ) engine=olap
    DUPLICATE KEY(pk, col_varchar_10__undef_signed)
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    """


    sql """
    insert into table_10_undef_partitions2_keys3_properties4_distributed_by53(pk,col_bigint_undef_signed,col_varchar_10__undef_signed,col_varchar_64__undef_signed) values (0,null,'right','g'),(1,-486256,'on','on'),(2,-1,'I''ll','at'),(3,29263,'h','don''t'),(4,5453,'a','s'),(5,-119,'j','can''t'),(6,89,'one','n'),(7,-7227,'s','u'),(8,94,'time','b'),(9,1816630,'yes','yes');
    """

    sql """set experimental_enable_pipeline_x_engine=true"""
    sql """set enable_local_shuffle = true"""

    qt_select """
    
    SELECT table1 . `pk` AS field1 , COUNT( DISTINCT table1 . `pk` ) AS field2 , table1 . `pk` AS field3 FROM  table_10_undef_partitions2_keys3_properties4_distributed_by52 AS table1  LEFT OUTER JOIN table_10_undef_partitions2_keys3_properties4_distributed_by52 AS table2 ON table2 . `pk`
<= table2 . `pk` LEFT  JOIN table_10_undef_partitions2_keys3_properties4_distributed_by5 AS table3 ON table2 . col_varchar_10__undef_signed = table2 . col_varchar_10__undef_signed WHERE   table1 . col_varchar_64__undef_signed > 'look' AND table1 . col_varchar_64__undef_signed <= 'zzzz' OR   table1 . col_varchar_10__undef_signed = table1 . col_varchar_10__undef_signed OR table1 . col_varchar_10__undef_signed > 'wmXlKwiRcZ' AND table1 . col_varchar_10__undef_signed <= 'z' AND   table1 . `pk`  IN ( 1 ) OR  table1 . `pk` = 4  GROUP BY field1,field3 order by field1;
    
    """


    sql """set experimental_enable_pipeline_x_engine=false"""
    sql """set enable_local_shuffle = false"""


    qt_select """
    
    SELECT table1 . `pk` AS field1 , COUNT( DISTINCT table1 . `pk` ) AS field2 , table1 . `pk` AS field3 FROM  table_10_undef_partitions2_keys3_properties4_distributed_by52 AS table1  LEFT OUTER JOIN table_10_undef_partitions2_keys3_properties4_distributed_by52 AS table2 ON table2 . `pk`
<= table2 . `pk` LEFT  JOIN table_10_undef_partitions2_keys3_properties4_distributed_by5 AS table3 ON table2 . col_varchar_10__undef_signed = table2 . col_varchar_10__undef_signed WHERE   table1 . col_varchar_64__undef_signed > 'look' AND table1 . col_varchar_64__undef_signed <= 'zzzz' OR   table1 . col_varchar_10__undef_signed = table1 . col_varchar_10__undef_signed OR table1 . col_varchar_10__undef_signed > 'wmXlKwiRcZ' AND table1 . col_varchar_10__undef_signed <= 'z' AND   table1 . `pk`  IN ( 1 ) OR  table1 . `pk` = 4  GROUP BY field1,field3 order by field1;
    
    """
     
}
