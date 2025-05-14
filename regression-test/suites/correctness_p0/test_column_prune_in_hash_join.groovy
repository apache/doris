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

suite("test_column_prune_in_hash_join") {
    sql """
        drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by5;
    """
    sql """
        drop table if exists table_50_undef_partitions2_keys3_properties4_distributed_by53;
    """
    
    sql """
        create table table_20_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed int  null  ,
        col_int_undef_signed_not_null int  not null  ,
        col_date_undef_signed date  null  ,
        col_date_undef_signed_not_null date  not null  ,
        col_varchar_5__undef_signed varchar(5)  null  ,
        col_varchar_5__undef_signed_not_null varchar(5)  not null  ,
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
        create table table_50_undef_partitions2_keys3_properties4_distributed_by53 (
        col_date_undef_signed date  null  ,
        col_int_undef_signed int  null  ,
        col_int_undef_signed_not_null int  not null  ,
        col_date_undef_signed_not_null date  not null  ,
        col_varchar_5__undef_signed varchar(5)  null  ,
        col_varchar_5__undef_signed_not_null varchar(5)  not null  ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_date_undef_signed, col_int_undef_signed)
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
    """

    sql """insert into table_20_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed_not_null,col_date_undef_signed,col_date_undef_signed_not_null,col_varchar_5__undef_signed,col_varchar_5__undef_signed_not_null) values (0,6,7625942,null,'2023-12-13','e','c'),(1,4,9,'2023-12-12','2023-12-12','m','w'),(2,null,-17559,'2023-12-10','2023-12-11','p','t'),(3,-15839,8,'2023-12-12','2023-12-12','h','d'),(4,null,5578188,'2023-12-17','2023-12-14','c','i'),(5,5,8,'2023-12-18','2023-12-14','t','n'),(6,1,8,'2023-12-10','2023-12-15','l','b'),(7,null,21615,null,'2023-12-18','j','b'),(8,4,3,'2018-07-06','2023-12-14','e','x'),(9,8,0,'2023-12-16','2016-03-07','c','n'),(10,1456,1,'2023-12-14','2013-03-25',null,'m'),(11,3909060,3,'2023-12-10','2023-12-16','c','l'),(12,3,5,'2023-12-15','2023-12-10','v','c'),(13,9,3,'2023-12-17','2023-12-14','z','f'),(14,-1054773,3,'2023-12-14','2023-12-10','x','v'),(15,null,25309,'2023-12-17','2023-12-16','k','f'),(16,6,5,'2013-12-28','2023-12-14',null,'f'),(17,-6209182,-28148,'2023-12-13','2023-12-10','k','a'),(18,4,7,'2023-12-15','2016-12-11','o','n'),(19,8,7,'2023-12-10','2023-12-18','g','w');"""
    sql """insert into table_50_undef_partitions2_keys3_properties4_distributed_by53(pk,col_int_undef_signed,col_int_undef_signed_not_null,col_date_undef_signed,col_date_undef_signed_not_null,col_varchar_5__undef_signed,col_varchar_5__undef_signed_not_null) values (0,2,4,'2023-12-17','2023-12-14','q','t'),(1,3,4,'2023-12-16','2023-12-14','s','j'),(2,-1553,9,'2023-12-17','2004-01-22','w','x'),(3,7,-15007,'2023-12-09','2023-12-14','i','y'),(4,10788,16430,'2023-12-11','2006-08-11','g','f'),(5,2,0,null,'2023-12-10',null,'a'),(6,4,1,'2023-12-16','2015-08-10',null,'l'),(7,3431600,2,null,'2023-12-18','d','q'),(8,6,3,null,'2023-12-17','u','i'),(9,8,10200,'2023-12-11','2023-12-13','e','p'),(10,19650,4,'2023-12-18','2023-12-09','w','w'),(11,7,9,'2023-12-16','2008-12-19','p','a'),(12,8,9,'2012-12-21','2023-12-13','t','q'),(13,31358,12458,'2023-12-15','2023-12-11','w','x'),(14,25240,7621716,'2023-12-14','2023-12-15','j','h'),(15,null,8,'2023-12-15','2023-12-10','b','g'),(16,0,0,'2023-12-13','2023-12-11','g','y'),(17,0,6408143,'2023-12-14','2023-12-12','e','w'),(18,null,4103654,null,'2023-12-18','n','r'),(19,7,12858,'2012-01-08','2023-12-16','s','j'),(20,4060964,9,'2023-12-11','2023-12-14','w','t'),(21,-6754922,4179,'2023-12-17','2012-02-28',null,'i'),(22,-3116224,-15938,'2023-12-13','2023-12-09','v','d'),(23,3,17007,'2023-12-14','2023-12-09','x','c'),(24,30889,23455,'2023-12-17','2002-11-11','w','o'),(25,6,4,'2019-09-18','2010-05-11','o','t'),(26,6,7,'2006-02-12','2023-12-10','l','a'),(27,-26679,1,null,'2023-12-11','t','r'),(28,null,5,'2023-12-10','2023-12-15','c','n'),(29,19637,5,'2015-03-07','2023-12-11','s','u'),(30,6,-6048,'2023-12-11','2023-12-15','o','v'),(31,null,-7483871,'2023-12-09','2023-12-17',null,'a'),(32,-4473089,6,'2014-05-27','2023-12-18','a','n'),(33,0,1901491,'2023-12-14','2023-12-11','h','w'),(34,0,3,'2023-12-14','2023-12-10',null,'g'),(35,0,-7316,'2023-12-13','2023-12-09',null,'f'),(36,9,5673837,'2023-12-09','2023-12-16','m','e'),(37,-350595,8,'2006-03-12','2011-10-07',null,'p'),(38,8,0,'2010-07-15','2023-12-15',null,'o'),(39,4,3774315,'2023-12-16','2023-12-12','c','t'),(40,4382800,5,'2023-12-12','2023-12-13','c','k'),(41,-2884860,5,'2023-12-14','2002-09-09','z','z'),(42,3,8,'2006-06-24','2007-03-09','j','v'),(43,null,2,'2023-12-15','2023-12-15','x','v'),(44,7041,4,null,'2023-12-09','z','c'),(45,null,3,'2023-12-12','2023-12-14','z','m'),(46,6,1,null,'2023-12-11','z','y'),(47,0,3,'2023-12-09','2023-12-16','p','j'),(48,6,8,'2023-12-18','2023-12-16',null,'h'),(49,null,-1907585,'2023-12-17','2023-12-17','c','v');"""

    qt_sql_prune_all_mark_join """
        SELECT
            9
        FROM
            table_20_undef_partitions2_keys3_properties4_distributed_by5 AS tbl_alias2
        WHERE
            (
                NOT (
                    tbl_alias2.col_int_undef_signed NOT IN (
                        SELECT
                            8
                        FROM
                            table_50_undef_partitions2_keys3_properties4_distributed_by53
                    )
                    AND  '2023-12-12' IN ('2023-12-19')
                )
            );
    """
    
    qt_sql_prune_other_conjuncts """
            SELECT
                9
            FROM
                table_20_undef_partitions2_keys3_properties4_distributed_by5 AS tbl_alias2
            WHERE
                (
                    NOT (
                        tbl_alias2.col_int_undef_signed NOT IN (
                            SELECT
                                8
                            FROM
                                table_50_undef_partitions2_keys3_properties4_distributed_by53
                        )
                    )
                );
    """

    qt_sql_prune_all """
            SELECT
                9
            FROM
                table_20_undef_partitions2_keys3_properties4_distributed_by5 AS tbl_alias2
            WHERE
                (
                    tbl_alias2.col_int_undef_signed NOT IN (
                        SELECT
                            8
                        FROM
                            table_50_undef_partitions2_keys3_properties4_distributed_by53
                    )
                );
    """
}
