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

suite("adjust_nullable") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS agg_subquery_table
       """
    sql """
    CREATE TABLE IF NOT EXISTS agg_subquery_table(
      gid varchar(50) NOT NULL,
      num int(11) SUM NOT NULL DEFAULT "0",
      id_bitmap bitmap BITMAP_UNION NOT NULL
    ) ENGINE = OLAP
    AGGREGATE KEY(gid)
    DISTRIBUTED BY HASH(gid) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    INSERT INTO agg_subquery_table VALUES
      ('1',4,to_bitmap(7)),
      ('2',5,to_bitmap(8)),
      ('3',6,to_bitmap(9));
    """

    sql """sync"""

    qt_select """
    SELECT
      subq_1.gid AS c0
    FROM
      agg_subquery_table AS subq_1
    WHERE
      EXISTS (
        SELECT
          ref_2.amt AS c2
        FROM (
          SELECT
            bitmap_union_count(id_bitmap) AS unt,
            sum(num) AS amt
          FROM
            agg_subquery_table
        ) AS ref_2
      )
    order by
      subq_1.gid;
    """

    sql """
        drop table if exists table_7_undef_undef;
    """
    sql """
        drop table if exists table_8_undef_undef;
    """
    sql """
        create table table_7_undef_undef (`pk` int,`col_int_undef_signed` int  ,`col_varchar_10__undef_signed` varchar(10)  ,`col_varchar_1024__undef_signed` varchar(1024)  ) engine=olap distributed by hash(pk) buckets 10 properties(    'replication_num' = '1');
    """
    sql """
        create table table_8_undef_undef (`pk` int,`col_int_undef_signed` int  ,`col_varchar_10__undef_signed` varchar(10)  ,`col_varchar_1024__undef_signed` varchar(1024)  ) engine=olap distributed by hash(pk) buckets 10 properties(    'replication_num' = '1');
    """

    sql """
        insert into table_7_undef_undef values (0,6,"didn't","was"),(1,1,"mean",'k'),(2,2,'i','i'),(3,null,'y','p'),(4,8,"you're","and"),(5,6,'i','o'),(6,null,"have","not");
    """
    sql """
        insert into table_8_undef_undef values (0,null,"one",'m'),(1,null,"got",'m'),(2,9,'m','b'),(3,null,"say",'p'),(4,null,'t',"yeah"),(5,null,'y',"because"),(6,null,"from",'q'),(7,null,"the","in");
    """

    qt_distinct_sum """
        SELECT    SUM( DISTINCT alias1 . `col_int_undef_signed` ) AS field1 FROM  table_7_undef_undef AS alias1  LEFT  JOIN table_8_undef_undef AS alias2 ON alias1 . `col_varchar_1024__undef_signed` = alias2 . `col_varchar_1024__undef_signed`  WHERE alias2 . `pk` >= alias2 . `col_int_undef_signed`  HAVING field1 <> 8 ORDER BY field1  , field1  ; 
    """

    sql """
        drop table if exists table_7_undef_undef;
    """
    sql """
        drop table if exists table_8_undef_undef;
    """

    sql """
        drop table if exists orders_2_x;
    """

    sql """CREATE TABLE `orders_2_x` (
        `o_orderdate` DATE not NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`o_orderdate`)
        DISTRIBUTED BY HASH(`o_orderdate`) BUCKETS 96
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""

    explain {
            sql("verbose insert into orders_2_x values ( '2023-10-17'),( '2023-10-17');")
            notContains("nullable=true")
    }
}

