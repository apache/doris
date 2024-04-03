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

suite("test_assert_num_rows") {
    qt_sql_1 """
        SELECT * from numbers("number"="10") WHERE  ( SELECT * FROM (SELECT 3) __DORIS_DUAL__ ) IS  NULL
    """

    qt_sql_2 """
        SELECT * from numbers("number"="10") WHERE  ( SELECT * FROM (SELECT 3) __DORIS_DUAL__ ) IS NOT NULL
    """
    sql """
        DROP TABLE IF EXISTS table_9_undef_undef;
    """
    sql """
        DROP TABLE IF EXISTS table_10_undef_undef;
    """
    sql """
    CREATE TABLE table_10_undef_undef (
        `pk` int, `col_int_undef_signed` int ,
        `col_varchar_10__undef_signed` varchar(10),
        `col_varchar_1024__undef_signed` varchar(1024))
    ENGINE=olap distributed BY hash(pk) buckets 10 properties('replication_num'='1');
    """

    sql """
    CREATE TABLE table_9_undef_undef (
        `pk` int,`col_int_undef_signed` int ,
        `col_varchar_10__undef_signed` varchar(10) ,
        `col_varchar_1024__undef_signed` varchar(1024))
    ENGINE=olap distributed BY hash(pk) buckets 10 properties('replication_num' = '1');
    """

    sql """
        INSERT INTO table_9_undef_undef
        VALUES (0,NULL,"get",'r'),
            (1,NULL,'q','i'),
            (2,2,"about","yes"),
            (3,NULL,"see","see"),
            (4,NULL,"was","been"),
            (5,NULL,"yes",'p'),
            (6,6,"you",'u'),
            (7,0,"me",'v'),
            (8,5,"something",'f');
    """
    sql """
        INSERT INTO table_10_undef_undef
        VALUES (0,NULL,"it's","time"),
            (1,NULL,"right",'o'),
            (2,5,'y','k'),
            (3,1,'r',"I'll"),
            (4,2,'e',"time"),
            (5,8,'v',"from"),
            (6,NULL,"you",'v'),
            (7,NULL,'r','a'),
            (8,1,'d',"didn't"),
            (9,NULL,'r',"go");
    """

    qt_sql_3 """
        SELECT alias1 . `pk` AS field1,
            alias1 . `col_int_undef_signed` AS field2
        FROM table_10_undef_undef AS alias1,
            table_9_undef_undef AS alias2
        WHERE
            (SELECT *
            FROM
            (SELECT 3) __DORIS_DUAL__) IS NULL
        HAVING field2 < 2
        ORDER BY alias1 . `pk`,
                alias1 .`pk` ASC,
                field1,
                field2
        LIMIT 2
        OFFSET 6;
    """
}