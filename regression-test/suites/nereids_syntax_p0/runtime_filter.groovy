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

suite("runtime_filter") {
    sql "drop table if exists date_v2_table"
    sql "drop table if exists datetime_table"

    sql """CREATE TABLE `date_v2_table` (
        `user_id` largeint(40) NOT NULL COMMENT '用户id',
        `date` dateV2 NOT NULL COMMENT '数据灌入日期时间'
    ) distributed by hash(user_id) buckets 1 
    properties("replication_num"="1");
    """

    sql """CREATE TABLE `datetime_table` (
        `user_id` largeint(40) NOT NULL COMMENT '用户id',
        `date` datetime NOT NULL COMMENT '数据灌入日期时间'
    ) distributed by hash(user_id) buckets 1 
    properties("replication_num"="1");
    """

    sql "insert into `date_v2_table` values (1, '2011-01-01'), (2, '2011-02-02');"
    sql "insert into `datetime_table` values (1, '2011-01-01 13:00:00'), (2, '2011-02-02 00:00:00');"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "set runtime_filter_type=1"
    test {
        sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
        result([[1L]])
    }
    sql "set runtime_filter_type=2"
    test {
        sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
        result([[1L]])
    }
    sql "set runtime_filter_type=4"
    test {
        sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
        result([[1L]])
    }
    sql "set runtime_filter_type=5"
    test {
        sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
        result([[1L]])
    }
    sql "set runtime_filter_type=6"
    test {
        sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
        result([[1L]])
    }
    sql "set runtime_filter_type=8"
    test {
        sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
        result([[1L]])
    }
    sql "set runtime_filter_type=12"
    test {
        sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
        result([[1L]])
    }

    multi_sql """

    drop table if exists runtime_filter_table_1111;
    create table runtime_filter_table_1111 (
    col_int_undef_signed int/*agg_type_placeholder*/   ,
    col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
    pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into runtime_filter_table_1111(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,1,null),(1,7,null);

    drop table if exists runtime_filter_table_2222;
    create table runtime_filter_table_2222 (
    col_int_undef_signed int/*agg_type_placeholder*/   ,
    col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
    pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into runtime_filter_table_2222(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,'think'),(1,null,''),(2,2,''),(3,null,'r'),(4,null,null),(5,8,'here');



    drop table if exists runtime_filter_table_3333;
    create table runtime_filter_table_3333 (
    col_int_undef_signed int/*agg_type_placeholder*/   ,
    col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
    pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into runtime_filter_table_3333(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,0,null),(1,6,null),(2,9,null),(3,2,''),(4,null,'here'),(5,null,'i'),(6,null,'now'),(7,5,'c'),(8,null,'t');


    drop table if exists runtime_filter_4444;
    create table runtime_filter_4444 (
    col_int_undef_signed int/*agg_type_placeholder*/   ,
    col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
    pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into runtime_filter_4444(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,'my'),(1,null,'a'),(2,5,''),(3,0,'that'),(4,0,'want'),(5,null,'g'),(6,null,null),(7,null,''),(8,null,null),(9,3,'b'),(10,null,'her'),(11,6,''),(12,null,'k'),(13,null,'then'),(14,2,null),(15,null,''),(16,null,'g'),(17,null,'x'),(18,null,'d'),(19,null,null);

    drop table if exists runtime_filter_table_5555;
    create table runtime_filter_table_5555 (
    col_int_undef_signed int/*agg_type_placeholder*/   ,
    col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
    pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into runtime_filter_table_5555(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,3,'s'),(1,8,''),(2,null,null),(3,7,'k'),(4,null,'x'),(5,null,''),(6,null,'will'),(7,null,'so');

    """
    //do not push rf inside recursive ctes
    // rule OR_EXPANSION genereates recursive ctes
    order_qt_rqg """
    SELECT
        *
    FROM
        runtime_filter_table_1111 AS t1 RIGHT SEMI
        JOIN runtime_filter_table_2222 AS t2 ON t1.`pk` + 1 = t2.`pk` + 1
        LEFT OUTER JOIN runtime_filter_table_5555 AS alias1 ON t2.`pk` + 5 = alias1.`pk`
        OR t2.`pk` + 4 = alias1.`pk` + 1
        INNER JOIN runtime_filter_table_3333 AS alias2
        INNER JOIN runtime_filter_4444 AS alias3 ON alias2.`pk` = alias3.`pk`;    
     """

    // do not generate rf on schemaScan. if rf generated, following sql is blocked
     multi_sql """
     set runtime_filter_mode=true;
     SELECT *
        FROM(
        SELECT tab.TABLE_SCHEMA, tab.TABLE_NAME
        FROM information_schema.TABLES tab
        WHERE TABLE_TYPE in ('BASE TABLE', 'SYSTEM VIEW')
        AND tab.TABLE_SCHEMA in ('__internal_schema')
        AND tab.TABLE_NAME IN ('audit_log')
        ORDER BY tab.TABLE_SCHEMA, tab.TABLE_NAME LIMIT 0, 100) t inner join information_schema.COLUMNS col on t.TABLE_SCHEMA=col.TABLE_SCHEMA
        AND t.TABLE_NAME=col.TABLE_NAME
        ORDER BY col.TABLE_SCHEMA,col.TABLE_NAME,col.ORDINAL_POSITION;
    """

}
