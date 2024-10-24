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

/**
 * Test delete with strange name
 */
suite("test_delete_handler") {
    // test condition operator
    sql """drop table if exists td1;"""
    sql """
    CREATE TABLE `td1` (
      `id` int(11) NULL,
      `name` varchar(255) NULL,
      `score` int(11) NULL
    ) ENGINE=OLAP
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_num" = "1" );
    """

    sql """insert into td1(id,name,`score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""
    sql """insert into td1(id,name,`score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""
    sql """insert into td1(id,name,`score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""

    sql """delete from td1 where `score` = 4 and `score` = 3 and `score` = 1;"""
    sql """delete from td1 where `score` is null;"""
    sql """delete from td1 where `score` is not null;"""
    sql """delete from td1 where `score` in (1,2);"""
    sql """delete from td1 where `score` not in (3,4);"""
    sql """select * from td1;"""
    sql """insert into td1(id,name,`score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""
    sql """select * from td1;"""


    // test column name
    sql """drop table if exists td2;"""
    sql """
    CREATE TABLE `td2` (
      `id` int(11) NULL,
      `name` varchar(255) NULL,
      `@score` int(11) NULL,
      `scoreIS` int(11) NULL,
      `sc ore` int(11) NULL,
      `score IS score` int(11) NULL,
      `_a-zA-Z0-9_.+-/?@#\$%^&*" ,:` int(11) NULL
    ) ENGINE=OLAP
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_num" = "1" );
    """

    sql """insert into td2(id,name,`@score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""
    sql """insert into td2(id,name,`@score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""
    sql """insert into td2(id,name,`@score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""

    sql """select * from td2;"""
    sql """delete from td2 where `@score` = 88;"""
    sql """delete from td2 where `scoreIS` is null;"""
    sql """delete from td2 where `score IS score` is null;"""
    sql """delete from td2 where `sc ore` is null;"""
    sql """delete from td2 where `_a-zA-Z0-9_.+-/?@#\$%^&*" ,:` is null;"""
    sql """delete from td2 where `_a-zA-Z0-9_.+-/?@#\$%^&*" ,:` is not null;"""
    sql """delete from td2 where `_a-zA-Z0-9_.+-/?@#\$%^&*" ,:` in (1,2,3);"""
    sql """delete from td2 where `_a-zA-Z0-9_.+-/?@#\$%^&*" ,:` not in (1,2,3);"""
    sql """select * from td2;"""
    sql """insert into td2(id,name,`@score`) values(1,"a",1),(2,"a",2),(3,"a",3),(4,"b",4);"""
    sql """select * from td2;"""

}

