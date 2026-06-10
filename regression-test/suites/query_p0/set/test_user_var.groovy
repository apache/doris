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

suite("test_user_var") {
    sql "SET @a1=1, @a2=0, @a3=-1"
    sql "SET @b1=1.1, @b2=0.0, @b3=-1.1"
    sql "SET @c1='H', @c2=''"
    sql "SET @d1=true, @d2=false"
    sql "SET @f1=null"

    qt_select1 'select @a1, @a2, @a3;'
    qt_select2 'select @b1, @b2, @b3;'
    qt_select3 'select @c1, @c2;'
    qt_select4 'select @d1, @d2;'
    qt_select5 'select @f1, @f2;'

    sql "SET @A1=2"
    qt_select6 'select @a1'
    sql "SET @a1 = 1"
    qt_select7 'select @A1'

    sql """drop table if exists orders"""

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       INTEGER NOT NULL,
      o_custkey        INTEGER NOT NULL,
      o_orderstatus    CHAR(1) NOT NULL,
      o_totalprice     DECIMALV3(15,2) NOT NULL,
      o_orderdate      DATE NOT NULL,
      o_orderpriority  CHAR(15) NOT NULL,  
      o_clerk          CHAR(15) NOT NULL, 
      o_shippriority   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi'); 
    """

    sql """set @ship_no = 1; """

    // o_shippriority AND @ship_no type should be int, should not add cast to type coercion
    explain {
        sql """select * from orders where o_shippriority = @ship_no;"""
        notContains "cast"
        notContains "CAST"
    }

    multi_sql """
    drop table if exists table_50_undef_partitions2_keys3_properties4_distributed_by54;
    create table table_50_undef_partitions2_keys3_properties4_distributed_by54 (
    col_date_undef_signed date  null  ,
    col_int_undef_signed int  null  ,
    col_datetime_undef_signed datetime  null  ,
    pk int
    ) engine=olap
    DUPLICATE KEY(col_date_undef_signed)
    PARTITION BY             RANGE(col_date_undef_signed) (
                    PARTITION p0 VALUES LESS THAN ('2023-12-11'),
                    PARTITION p1 VALUES LESS THAN ('2023-12-15'),
                    PARTITION p2 VALUES LESS THAN ('2023-12-16'),
                    PARTITION p3 VALUES LESS THAN ('2023-12-17'),
                    PARTITION p4 VALUES LESS THAN ('2024-01-18'),
                    PARTITION p5 VALUES LESS THAN ('2026-02-18'),
                    PARTITION p100 VALUES LESS THAN ('9999-12-31')
                )
            
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into table_50_undef_partitions2_keys3_properties4_distributed_by54(pk,col_int_undef_signed,col_date_undef_signed,col_datetime_undef_signed) values (0,3,'2023-12-09','2004-12-17 12:35:40'),(1,null,'2023-12-15','2007-03-09 11:14:00'),(2,9,'2023-12-15','2005-01-10 12:14:20'),(3,1,null,'2012-08-13 07:51:37'),(4,5,'2023-12-12','2008-06-09 05:14:31'),(5,5,null,'2003-12-12 03:46:03'),(6,4,'2012-11-08','2013-05-17 19:32:15'),(7,5,'2023-12-15','2013-11-11 05:02:08'),(8,7,'2023-12-15','2001-07-01 20:59:59'),(9,9,'2023-12-14','2007-05-11 16:10:53'),(10,null,'2023-12-17','2014-12-22 08:07:07'),(11,9,null,'2019-12-26 12:33:12'),(12,null,'2023-12-12','2018-02-04 20:16:03'),(13,1,'2023-12-17','2012-12-23 09:29:23'),(14,null,'2023-12-14','2002-01-23 18:40:57'),(15,0,'2023-12-13','2005-03-20 23:57:50'),(16,7,'2023-12-15','2005-04-22 08:56:50'),(17,8,'2023-12-18','2019-06-27 11:46:05'),(18,0,'2023-12-17','2000-10-16 00:48:54'),(19,null,'2023-12-16','2010-02-18 12:33:42'),(20,9,'2023-12-16','2011-06-23 14:58:11'),(21,null,null,'2009-09-22 23:43:05'),(22,6,'2023-12-14','2004-01-12 10:08:48'),(23,3,null,'2008-08-23 17:12:45'),(24,2,'2023-12-16','2013-10-14 01:57:45'),(25,0,null,'2012-12-05 06:31:29'),(26,8,'2023-12-13','2014-04-03 17:49:28'),(27,null,'2023-12-11','2014-04-07 19:37:58'),(28,1,'2023-12-18','2014-09-18 14:09:04'),(29,8,'2023-12-14','2004-09-18 16:57:38'),(30,null,'2023-12-13','2014-10-10 01:44:04'),(31,9,'2023-12-15','2017-09-24 12:33:16'),(32,2,'2023-12-16','2013-11-15 17:12:36'),(33,4,'2005-04-24','2003-03-06 08:47:44'),(34,6,'2023-12-16','2001-05-21 10:40:16'),(35,2,'2023-12-17','2001-06-02 02:34:54'),(36,4,'2023-12-14','2010-03-06 08:46:49'),(37,4,null,'2000-04-08 07:56:25'),(38,8,'2023-12-10','2018-03-24 22:17:31'),(39,0,'2023-12-11','2009-12-13 17:40:47'),(40,9,'2023-12-16','2014-05-04 01:17:18'),(41,5,'2017-02-23','2005-04-10 15:34:29'),(42,2,'2023-12-15','2013-12-16 10:12:23'),(43,6,'2023-12-12','2000-07-28 14:30:17'),(44,7,'2023-12-11','2002-03-24 11:43:04'),(45,7,'2023-12-09','2012-07-25 19:06:56'),(46,null,'2005-12-05','2018-10-07 21:28:38'),(47,1,'2023-12-15','2019-06-15 23:13:02'),(48,4,null,'2012-10-23 02:55:35'),(49,5,'2023-12-17','2009-09-07 11:50:21');
    SET @v0 = 2502512601;
    SET @v1 = 'default';
    SET @v2 = '2025-06-18';
    SET @v3 = '2023-12-17';
    """
    qt_select_user_var """SELECT WINDOW_FUNNEL(@v0, @v1, col_datetime_undef_signed, col_date_undef_signed  !=  @v2, col_date_undef_signed  >  @v3) FROM table_50_undef_partitions2_keys3_properties4_distributed_by54 ;"""
}