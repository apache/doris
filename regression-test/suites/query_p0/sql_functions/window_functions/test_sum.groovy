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

suite("test_sum") {
    qt_select """
                  select k1, sum(k5) over 
                      (partition by k1 order by k3 range between current row and unbounded following) as w 
                  from test_query_db.test order by k1, w
              """
    sql "create database if not exists multi_db"
    sql "use multi_db"
    sql "DROP TABLE IF EXISTS null_table"
    sql """
        CREATE TABLE null_table (
            id int,
            v1 int,
            v2 varchar,
            v3 varchar
            ) ENGINE = OLAP
            DUPLICATE KEY(id) COMMENT 'OLAP'
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """ 
    sql """
            INSERT INTO null_table (id, v1, v2, v3) VALUES
            (0, NULL, 'be', NULL),
            (12, NULL, 'be', NULL),
            (6, NULL, 'but', NULL),
            (11, NULL, 'did', NULL),
            (14, NULL, 'go', NULL),
            (15, NULL, 'go', NULL),
            (7, NULL, 'had', NULL),
            (13, NULL, 'it', NULL),
            (19, NULL, 'look', NULL),
            (9, NULL, 'not', NULL),
            (10, NULL, 'of', NULL),
            (16, NULL, 'right', NULL),
            (17, NULL, 'say', NULL),
            (3, NULL, 'she', NULL),
            (8, NULL, 'she', NULL),
            (4, NULL, 'some', NULL),
            (5, NULL, 'there', NULL),
            (18, NULL, 'we', NULL),
            (2, NULL, 'well', NULL),
            (1, NULL, 'who', NULL);
        """ 

    qt_sql_window_null """   select id,v1,v2, max(v1) over(partition by v2 order by id rows between 7 preceding and current row) from null_table order by v2,id; """

    sql "create database if not exists multi_db"
    sql "use multi_db"
    sql "DROP TABLE IF EXISTS multi"
    sql """
        CREATE TABLE multi (
            id int,
            v1 int,
            v2 varchar
            ) ENGINE = OLAP
            DUPLICATE KEY(id) COMMENT 'OLAP'
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """ 
    sql """
        insert into multi values (1, 1, 'a'),(1, 1, 'a'), (2, 1, 'a'), (3, 1, 'a');
        """ 
    qt_sql_window_muti1 """   select multi_distinct_group_concat(v2) over() from multi; """
    qt_sql_window_muti2 """   select multi_distinct_sum(v1) over() from multi; """
    qt_sql_window_muti3 """   select multi_distinct_count(v1) over() from multi; """

    sql "DROP TABLE IF EXISTS table_20_undef_partitions2_keys3_properties4_distributed_by5"
    sql """
        create table table_20_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed int  null  ,
        col_date_undef_signed date  null  ,
        col_datetime_undef_signed datetime  null  ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed)
        PARTITION BY             RANGE(col_int_undef_signed) (
                        PARTITION p0 VALUES LESS THAN ('4'),
                        PARTITION p1 VALUES LESS THAN ('6'),
                        PARTITION p2 VALUES LESS THAN ('7'),
                        PARTITION p3 VALUES LESS THAN ('8'),
                        PARTITION p4 VALUES LESS THAN ('10'),
                        PARTITION p5 VALUES LESS THAN ('83647'),
                        PARTITION p100 VALUES LESS THAN ('2147483647')
                    )
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """ 
    sql """
        insert into table_20_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_date_undef_signed,col_datetime_undef_signed) values (0,2,'2023-12-14','2003-02-28 20:24:41'),(1,null,'2001-10-09','2004-03-02 17:09:05'),(2,2,'2023-12-13','2009-04-11 22:14:33'),(3,5,null,'2012-05-13 00:29:32'),(4,null,null,'2012-05-04 08:43:40'),(5,0,'2023-12-18','2010-12-10 13:18:28'),(6,4,'2023-12-11','2006-06-24 13:51:23'),(7,0,'2023-12-15','2006-03-10 01:56:00'),(8,null,'2023-12-12','2002-03-03 13:32:25'),(9,0,'2023-12-13','2019-02-23 19:21:54'),(10,7,'2023-12-16','2010-08-24 04:58:24'),(11,9,'2023-12-18','2004-11-05 09:02:56'),(12,null,'2023-12-09','2003-12-04 01:28:26'),(13,null,'2023-12-18','2007-02-11 02:36:58'),(14,null,'2023-12-11','2005-06-02 14:15:22'),(15,5,'2023-12-18','2008-11-09 12:15:47'),(16,null,'2023-12-11','2005-09-27 02:45:04'),(17,7,'2023-12-15','2009-10-20 13:31:42'),(18,null,'2023-12-14','2015-02-16 02:57:57'),(19,8,'2023-12-13','2008-02-04 19:25:53');
    """ 
    qt_sql_window_null1 """   SELECT
                pk,
                col_int_undef_signed,
                col_date_undef_signed,
                col_datetime_undef_signed,
                MIN(col_int_undef_signed) OVER (
                    PARTITION BY col_int_undef_signed
                    ORDER BY
                        col_int_undef_signed,
                        col_date_undef_signed,
                        col_datetime_undef_signed,
                        pk DESC ROWS BETWEEN 9 PRECEDING
                        AND 9 FOLLOWING
                ) as min
            FROM
                table_20_undef_partitions2_keys3_properties4_distributed_by5
                where col_int_undef_signed is null order by pk; 
    """

    qt_sql_window_null2 """   SELECT
                pk,
                col_int_undef_signed,
                col_date_undef_signed,
                col_datetime_undef_signed,
                MIN(col_int_undef_signed) OVER (
                    PARTITION BY col_int_undef_signed
                    ORDER BY
                        col_int_undef_signed,
                        col_date_undef_signed,
                        col_datetime_undef_signed,
                        pk DESC ROWS BETWEEN 9 PRECEDING
                        AND 9 FOLLOWING
                ) as min
            FROM
                table_20_undef_partitions2_keys3_properties4_distributed_by5
            order by
                col_int_undef_signed,
                col_date_undef_signed,
                col_datetime_undef_signed,
                pk DESC;
    """
}

