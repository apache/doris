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

suite("test_subquery") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    qt_sql1 """
        select c1, c3, m2 from 
            (select c1, c3, max(c2) m2 from 
                (select c1, c2, c3 from 
                    (select k3 c1, k2 c2, max(k1) c3 from nereids_test_query_db.test 
                     group by 1, 2 order by 1 desc, 2 desc limit 5) x 
                ) x2 group by c1, c3 limit 10
            ) t 
        where c1>0 order by 2 , 1 limit 3
    """

    qt_sql2 """
        with base as (select k1, k2 from nereids_test_query_db.test as t where k1 in (select k1 from nereids_test_query_db.baseall
        where k7 = 'wangjuoo4' group by 1 having count(distinct k7) > 0)) select * from base limit 10;
    """

    qt_sql3 """
        SELECT k1 FROM nereids_test_query_db.test GROUP BY k1 HAVING k1 IN (SELECT k1 FROM nereids_test_query_db.baseall WHERE
        k2 >= (SELECT min(k3) FROM nereids_test_query_db.bigtable WHERE k2 = baseall.k2)) order by k1;
    """

    qt_sql4 """
        select count() from (select k2, k1 from nereids_test_query_db.baseall order by k1 limit 1) a;
    """

    qt_uncorrelated_exists_with_limit_0 """
        select * from nereids_test_query_db.baseall where exists (select * from nereids_test_query_db.baseall limit 0)
    """

    // test uncorrelated scalar subquery with limit <= 1
    sql """
        select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall limit 1)
    """

    // test uncorrelated subquery in having
    sql """
        select count(*) from nereids_test_query_db.baseall
        group by k0 
        having min(k0) in (select k0 from nereids_test_query_db.baseall)
    """

    // test uncorrelated scalar subquery with more than one return rows
    test {
        sql """
            select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall limit 2)
        """
        exception("Expected LE 1 to be returned by expression")
    }

    // test uncorrelated scalar subquery with order by and limit
    qt_uncorrelated_scalar_with_sort_and_limit """
            select * from nereids_test_query_db.baseall where k1 = (select k1 from nereids_test_query_db.baseall order by k1 desc limit 1)
        """

    sql """DROP TABLE IF EXISTS table_1000_undef_undef"""
    sql """DROP TABLE IF EXISTS table_1000_undef_undef2"""
    sql """CREATE TABLE `table_1000_undef_undef` (
            `pk` int(11) NULL,
            `col_bigint_undef_signed` bigint(20) NULL,
            `col_bigint_undef_signed2` bigint(20) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`pk`, `col_bigint_undef_signed`, `col_bigint_undef_signed2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`pk`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );  """

    sql """ CREATE TABLE `table_1000_undef_undef2` (
            `pk` int(11) NULL,
            `col_bigint_undef_signed` bigint(20) NULL,
            `col_bigint_undef_signed2` bigint(20) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`pk`, `col_bigint_undef_signed`, `col_bigint_undef_signed2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`pk`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );"""
    explain {
        sql """
            SELECT `col_bigint_undef_signed` '00:39:36' , `col_bigint_undef_signed` '11:19:45', `col_bigint_undef_signed` '11:55:37', `col_bigint_undef_signed2` '19:01:23'
                FROM table_1000_undef_undef2
                WHERE EXISTS 
                    (SELECT `col_bigint_undef_signed` '17:38:13' , `col_bigint_undef_signed2` '17:36:21'
                    FROM table_1000_undef_undef2
                    WHERE `col_bigint_undef_signed2` NOT IN 
                        (SELECT `col_bigint_undef_signed`
                        FROM table_1000_undef_undef2
                        WHERE `col_bigint_undef_signed2` < 
                            (SELECT AVG(`col_bigint_undef_signed`)
                            FROM table_1000_undef_undef2
                            WHERE `col_bigint_undef_signed2` < 2)) ) ; 
        """
        contains("VAGGREGATE")
    }

    explain {
        sql """SELECT * FROM table_1000_undef_undef t1 WHERE t1.pk <= (SELECT COUNT(t2.pk) FROM table_1000_undef_undef2 t2 WHERE (t1.col_bigint_undef_signed = t2.col_bigint_undef_signed)); """
        contains("ifnull")
    }

    sql """DROP TABLE IF EXISTS table_1000_undef_undef"""
    sql """DROP TABLE IF EXISTS table_1000_undef_undef2"""

    sql """drop table if exists test_one_row_relation;"""
    sql """
        CREATE TABLE `test_one_row_relation` (
        `user_id` int(11) NULL 
        )
        UNIQUE KEY(`user_id`)
        COMMENT 'test'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ set enable_nereids_dml=true; """
    
    sql """insert into test_one_row_relation select (select 1);"""

    qt_sql_subquery_one_row_relation """select * from test_one_row_relation;"""

    qt_sql_mark_join """with A as (select count(*) n1 from test_one_row_relation where exists (select 1 from test_one_row_relation t where t.user_id = test_one_row_relation.user_id) or 1 = 1) select * from A;"""

    sql """drop table if exists test_one_row_relation;"""

    sql """drop table if exists subquery_test_t1;"""
    sql """drop table if exists subquery_test_t2;"""
    sql """create table subquery_test_t1 (
                id int
            )
            UNIQUE KEY (`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""
    sql """create table subquery_test_t2 (
                id int
            )
            UNIQUE KEY (`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

    explain {
        sql("""analyzed plan select subquery_test_t1.id from subquery_test_t1
                where
                    not (
                            exists(select 1 from subquery_test_t2 where subquery_test_t1.id = subquery_test_t2.id and subquery_test_t2.id = 5) 
                            and
                            exists(select 1 from subquery_test_t2 where subquery_test_t1.id = subquery_test_t2.id and subquery_test_t2.id = 6)
                        ); """)
        contains("isMarkJoin=true")
    }
    explain {
        sql("""analyzed plan select subquery_test_t1.id from subquery_test_t1
                where
                    not ( 
                            subquery_test_t1.id > 10 
                            and 
                            exists(select 1 from subquery_test_t2 where subquery_test_t1.id = subquery_test_t2.id and subquery_test_t2.id = 6)
                        );""")
        contains("isMarkJoin=true")
    }
    explain {
        sql("""analyzed plan select subquery_test_t1.id from subquery_test_t1
                where
                    not ( 
                            subquery_test_t1.id > 10 
                            and 
                            subquery_test_t1.id in (select 1 from subquery_test_t2 where subquery_test_t1.id = subquery_test_t2.id and subquery_test_t2.id = 6)
                        );  """)
        contains("isMarkJoin=true")
    }
    explain {
        sql("""analyzed plan select subquery_test_t1.id from subquery_test_t1
                where
                    not ( 
                            subquery_test_t1.id > 10 
                            and 
                            subquery_test_t1.id in (select 1 from subquery_test_t2 where subquery_test_t1.id = subquery_test_t2.id and subquery_test_t2.id = 6)
                        ); """)
        contains("isMarkJoin=true")
    }
    explain {
        sql("""analyzed plan select subquery_test_t1.id from subquery_test_t1
                where
                    not ( 
                            subquery_test_t1.id > 10 
                            and 
                            ( subquery_test_t1.id < 100 or subquery_test_t1.id in (select 1 from subquery_test_t2 where subquery_test_t1.id = subquery_test_t2.id and subquery_test_t2.id = 6) )
                        ); """)
        contains("isMarkJoin=true")
    }
    explain {
        sql("""analyzed plan select subquery_test_t1.id from subquery_test_t1
                where
                    not ( 
                            subquery_test_t1.id > 10 
                            and 
                            ( subquery_test_t1.id < 100 or case when subquery_test_t1.id in (select 1 from subquery_test_t2 where subquery_test_t1.id = subquery_test_t2.id and subquery_test_t2.id = 6) then 1 else 0 end )
                        );""")
        contains("isMarkJoin=true")
    }

    sql """drop table if exists table_23_undef_undef"""
    sql """create table table_23_undef_undef (`pk` int,`col_int_undef_signed` int  ,`col_varchar_10__undef_signed` varchar(10)  ,`col_varchar_1024__undef_signed` varchar(1024)  ) engine=olap distributed by hash(pk) buckets 10 properties(    'replication_num' = '1');"""
    sql """drop table if exists table_20_undef_undef"""
    sql """create table table_20_undef_undef (`pk` int,`col_int_undef_signed` int  ,`col_varchar_10__undef_signed` varchar(10)  ,`col_varchar_1024__undef_signed` varchar(1024)  ) engine=olap distributed by hash(pk) buckets 10 properties(    'replication_num' = '1');"""
    sql """drop table if exists table_9_undef_undef"""
    sql """create table table_9_undef_undef (`pk` int,`col_int_undef_signed` int  ,`col_varchar_10__undef_signed` varchar(10)  ,`col_varchar_1024__undef_signed` varchar(1024)  ) engine=olap distributed by hash(pk) buckets 10 properties(    'replication_num' = '1');"""

    sql """insert into table_23_undef_undef values (0,0,'t','p'),(1,6,'q',"really"),(2,3,'p',"of"),(3,null,"he",'k'),(4,8,"this","don't"),(5,6,"see","this"),(6,5,'s','q'),(7,null,'o','j'),(8,9,'l',"could"),(9,null,"one",'l'),(10,7,"can't",'f'),(11,2,"going","not"),(12,null,'g','r'),(13,3,"ok",'s'),(14,6,"she",'k'),(15,null,"she",'p'),(16,8,"what","him"),(17,null,"from","to"),(18,5,"so","up"),(19,null,"my","is"),(20,null,'h',"see"),(21,null,"as","to"),(22,0,"know","the");"""
    sql """insert into table_20_undef_undef values (0,null,'r','x'),(1,null,'m',"say"),(2,2,"mean",'h'),(3,null,'n','b'),(4,8,"do","do"),(5,9,'h',"were"),(6,null,"was","one"),(7,2,'o',"she"),(8,0,"who","me"),(9,null,'n',"that"),(10,null,"will",'l'),(11,4,'m',"if"),(12,5,"the","got"),(13,null,"why",'f'),(14,0,"of","for"),(15,null,"or","ok"),(16,null,'c','u'),(17,3,'f','c'),(18,null,"see",'f'),(19,2,'f','z');"""
    sql """insert into table_9_undef_undef values (0,3,"his",'g'),(1,8,'p','n'),(2,null,"get","got"),(3,3,'r','r'),(4,null,"or","get"),(5,0,'j',"yeah"),(6,null,'w','x'),(7,8,'q',"for"),(8,3,'p',"that");"""

    qt_select_sub"""SELECT DISTINCT alias1.`pk` AS field1,     alias2.`col_int_undef_signed` AS field2 FROM table_23_undef_undef AS alias1,     table_20_undef_undef AS alias2 WHERE (         EXISTS (             SELECT DISTINCT SQ1_alias1.`col_varchar_10__undef_signed` AS SQ1_field1             FROM table_9_undef_undef AS SQ1_alias1             WHERE SQ1_alias1.`col_varchar_10__undef_signed` = alias1.`col_varchar_10__undef_signed`             )         )     OR alias1.`col_varchar_1024__undef_signed` = "TmxRwcNZHC"     AND (         alias1.`col_varchar_10__undef_signed` <> "rnZeukOcuM"         AND alias2.`col_varchar_10__undef_signed` != "dbPAEpzstk"         ) ORDER BY alias1.`pk`,     field1,     field2 LIMIT 2 OFFSET 7; """
    sql """drop table if exists table_23_undef_undef"""
    sql """drop table if exists table_20_undef_undef"""
    sql """drop table if exists table_9_undef_undef"""

}
