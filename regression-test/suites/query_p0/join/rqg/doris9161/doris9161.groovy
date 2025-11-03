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

suite("doris9161") {
    sql """
    DROP TABLE IF EXISTS `table_25_undef_partitions2_keys3`;
    """
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
    DROP TABLE IF EXISTS `table_7_undef_partitions2_keys3`;
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
        insert into table_25_undef_partitions2_keys3(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,4,'n'),(1,null,"on"),(2,4,null),(3,1,""),(4,6,""),(5,null,'e'),(6,1,"if"),(7,4,"don't"),(8,null,'q'),(9,null,"did"),(10,null,null),(11,4,null),(12,null,"a"),(13,3,null),(14,1,null),(15,8,null),(16,7,null),(17,null,"what"),(18,6,'l'),(19,null,""),(20,7,null),(21,null,"yes"),(22,0,null),(23,3,""),(24,8,null);
    """

    sql """
        insert into table_7_undef_partitions2_keys3(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,2,""),(1,null,'d'),(2,6,"like"),(3,null,"time"),(4,5,""),(5,9,""),(6,2,"a");
    """

    qt_sql """
        SELECT *
FROM table_25_undef_partitions2_keys3 AS t1
WHERE NOT t1.`pk` <= 7
    OR t1.`pk` >= 1
    OR t1.`pk` NOT IN (
        SELECT `pk`
        FROM table_7_undef_partitions2_keys3 AS t2
        WHERE t1.pk = t2.pk
    ) order by 1,2,3;
    """
}