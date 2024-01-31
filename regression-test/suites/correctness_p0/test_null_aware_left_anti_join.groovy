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

suite("test_null_aware_left_anti_join") {
    def tableName1 = "test_null_aware_left_anti_join1"
    def tableName2 = "test_null_aware_left_anti_join2"
    sql """
        drop table if exists ${tableName1};
    """

    sql """
        drop table if exists ${tableName2};
    """

    sql """
        create table if not exists ${tableName1} ( `k1` int(11) NULL ) DISTRIBUTED BY HASH(`k1`) BUCKETS 4         PROPERTIES (         "replication_num" = "1");
    """

    sql """
        create table if not exists ${tableName2} ( `k1` int(11) NULL ) DISTRIBUTED BY HASH(`k1`) BUCKETS 4         PROPERTIES (         "replication_num" = "1");
    """

    sql """
        insert into ${tableName1} values (1), (3);
    """

    sql """
        insert into ${tableName2} values (1), (2);
    """

    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    sql """
        insert into ${tableName2} values(null);
    """

    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    sql """
        insert into ${tableName1} values(null);
    """

    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    sql """ set parallel_fragment_exec_instance_num=2; """
    sql """ set parallel_pipeline_task_num=2; """
    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    // In left anti join, if right side is empty, all rows(null included) of left should be output.
    qt_anti_emtpy_right """
        select
            *
        from ${tableName1} t1 where k1 not in (
            select k1 from ${tableName2} t2 where t2.k1 > 2
        ) order by 1;
    """

    // In left semi join, if right side is empty, no row should be output.
    qt_semi_emtpy_right """
        select
            *
        from ${tableName1} t1 where k1 in (
            select k1 from ${tableName2} t2 where t2.k1 > 2
        ) order by 1;
    """

    qt_anti_emtpy_right2 """
        select
            *
        from ${tableName1} t1 where k1 not in (
            select k1 from ${tableName2} t2 where t2.k1 > 2
        ) or k1 > 5 order by 1;
    """

    sql """drop table if exists table_20_undef_undef;"""
    sql """drop table if exists table_100_undef_undef;"""
    sql """drop table if exists table_22_undef_undef;"""

    sql """
        create table table_20_undef_undef (
            `pk` int,
            `col_int_undef_signed_` int   ,
            `col_varchar_10__undef_signed_` varchar(10)   ,
            `col_varchar_1024__undef_signed_` varchar(1024)   
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties(
                'replication_num' = '1');
    """

    sql """
            create table table_100_undef_undef (
            `pk` int,
            `col_int_undef_signed_` int   ,
            `col_varchar_10__undef_signed_` varchar(10)   ,
            `col_varchar_1024__undef_signed_` varchar(1024)   
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties(
                'replication_num' = '1');
    """

    sql """
            create table table_22_undef_undef (
            `pk` int,
            `col_int_undef_signed_` int   ,
            `col_varchar_10__undef_signed_` varchar(10)   ,
            `col_varchar_1024__undef_signed_` varchar(1024)   
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties(
                'replication_num' = '1');
    """
    sql """
    insert into table_20_undef_undef values (0,1,'s','y'),(1,7,'q',"something"),(2,7,'s',"time"),(3,0,'l','q'),(4,null,'e',"was"),(5,5,"about",'v'),(6,5,'a',"you're"),(7,null,"if",'n'),(8,null,"of","to"),(9,null,'g','r'),(10,6,"good","like"),(11,null,'y','t'),(12,null,'f','i'),(13,null,"like","get"),(14,7,"as","going"),(15,null,"my","of"),(16,5,"could",'b'),(17,1,'s',"or"),(18,5,'w',"it's"),(19,null,'o','n');
    """
    sql """
    insert into table_100_undef_undef values (0,1,"he",'n'),(1,null,'x','v'),(2,3,"out","her"),(3,0,"ok",'e'),(4,null,'v',"now"),(5,null,'s',"of"),(6,8,'i',"i"),(7,1,"hey",'v'),(8,1,"for","yeah"),(9,6,"oh","or"),(10,null,'l',"his"),(11,null,"well","his"),(12,7,'l','n'),(13,null,"this","at"),(14,null,'b','i'),(15,null,"no",'j'),(16,8,"mean",'n'),(17,6,"on",'k'),(18,null,"really","at"),(19,null,'l','e'),(20,null,'h','u'),(21,null,"time",'y'),(22,null,'q','s'),(23,8,"could","one"),(24,null,"were","she"),(25,null,"he","time"),(26,null,'r','n'),(27,7,'a','r'),(28,7,"mean","good"),(29,null,"all",'i'),(30,7,"be",'i'),(31,null,"the","not"),(32,7,'p','u'),(33,8,"can't",'e'),(34,null,"so","think"),(35,null,"yeah","some"),(36,null,"she","think"),(37,null,"that's","have"),(38,3,"you're",'z'),(39,8,"out","in"),(40,null,"be",'r'),(41,4,'e',"that"),(42,8,"say",'y'),(43,0,'h','s'),(44,1,'o',"he"),(45,7,"yes",'p'),(46,null,'h','e'),(47,7,"at",'j'),(48,null,'o',"if"),(49,6,'f','l'),(50,1,'d',"out"),(51,8,"hey","because"),(52,null,"about",'h'),(53,null,'e',"see"),(54,9,"time",'s'),(55,null,'c','k'),(56,null,"so",'h'),(57,null,'q','e'),(58,null,"well","my"),(59,null,'m',"she"),(60,null,'n',"my"),(61,2,"he",'o'),(62,6,'g','i'),(63,6,'g',"out"),(64,3,"were","who"),(65,null,"yes","think"),(66,0,'s',"or"),(67,5,"her",'o'),(68,3,'v','p'),(69,7,"it",'k'),(70,5,"out",'x'),(71,null,'p',"one"),(72,null,"got",'c'),(73,4,'m',"want"),(74,null,'z',"think"),(75,5,'b','a'),(76,null,"back",'i'),(77,6,'u',"can"),(78,null,"going","no"),(79,0,'e','t'),(80,null,'s','s'),(81,null,"been",'z'),(82,8,'t',"my"),(83,2,'g',"the"),(84,7,"will","get"),(85,null,'e',"in"),(86,4,'t','f'),(87,null,'v',"something"),(88,6,'x','k'),(89,9,'i','q'),(90,4,"who","really"),(91,9,'z',"going"),(92,null,"tell","something"),(93,7,"at",'y'),(94,null,'h',"know"),(95,1,"can't","he"),(96,5,"hey","know"),(97,null,'q',"right"),(98,3,'a','m'),(99,5,'h','p');
    """
    sql """
    insert into table_22_undef_undef values (0,null,'z',"out"),(1,9,"was",'v'),(2,0,'w','i'),(3,7,'l',"it"),(4,9,"his",'b'),(5,null,"from","from"),(6,null,'h','h'),(7,8,'x',"then"),(8,9,'i','h'),(9,null,'o','k'),(10,1,'x',"to"),(11,null,'u',"something"),(12,null,'z',"of"),(13,null,'s',"been"),(14,5,"why",'o'),(15,3,"time","the"),(16,null,"would","not"),(17,null,"her","for"),(18,6,'w',"think"),(19,null,"at",'g'),(20,null,'z',"his"),(21,5,'l','t');
    """

    qt_select_1 """
        SELECT COUNT(*) FROM (SELECT    alias2 . `pk` AS field1 , alias1 . `pk` AS field2 , alias2 . `pk` AS field3 , alias1 . `pk` AS field4 , alias1 . `pk` AS field5 FROM table_20_undef_undef AS alias1 , table_100_undef_undef AS alias2 WHERE (  alias1 . `col_int_undef_signed_` NOT IN ( SELECT   SQ1_alias1 . `pk` AS SQ1_field1 FROM table_22_undef_undef AS SQ1_alias1    ) ) AND ( alias1 . `col_varchar_1024__undef_signed_` >= "I'm" OR alias1 . `col_varchar_1024__undef_signed_` >= "the" ) OR alias1 . `pk` <= alias2 . `col_int_undef_signed_`  HAVING field4 <= 8) AS T ;
    """

    sql """drop table if exists table_25_undef_partitions2_keys3;"""
    sql """drop table if exists table_7_undef_partitions2_keys3;"""
    sql """
                create table table_25_undef_partitions2_keys3 (
            `col_int_undef_signed` int   ,
            `col_varchar_10__undef_signed` varchar(10)   ,
            `pk` int
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties('replication_num' = '1');
    """
    sql """
        insert into table_25_undef_partitions2_keys3(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,4,'n'),(1,null,"on"),(2,4,null),(3,1,""),(4,6,""),(5,null,'e'),(6,1,"if"),(7,4,"don't"),(8,null,'q'),(9,null,"did"),(10,null,null),(11,4,null),(12,null,"a"),(13,3,null),(14,1,null),(15,8,null),(16,7,null),(17,null,"what"),(18,6,'l'),(19,null,""),(20,7,null),(21,null,"yes"),(22,0,null),(23,3,""),(24,8,null);
    """
    sql """
            create table table_7_undef_partitions2_keys3 (
            `col_int_undef_signed` int   ,
            `col_varchar_10__undef_signed` varchar(10)   ,
            `pk` int
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties('replication_num' = '1');
    """

    sql """
        insert into table_7_undef_partitions2_keys3(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,2,""),(1,null,'d'),(2,6,"like"),(3,null,"time"),(4,5,""),(5,9,""),(6,2,"a");
    """

    qt_select_2 """
        SELECT *
        FROM table_25_undef_partitions2_keys3 AS t1
        WHERE NOT t1.`pk` <= 7
            OR t1.`pk` >= 1
            OR t1.`pk` NOT IN (
                SELECT `pk`
                FROM table_7_undef_partitions2_keys3 AS t2
                WHERE t1.pk = t2.pk
            ) order by pk;
    """
}
