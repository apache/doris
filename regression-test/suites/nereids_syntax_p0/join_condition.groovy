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

suite("join_condition") {
    multi_sql """
        drop table if exists join_condition_table_1111;
        create table join_condition_table_1111 (
        col_int_undef_signed int/*agg_type_placeholder*/   ,
        col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
        pk int/*agg_type_placeholder*/
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into join_condition_table_1111(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,0,null),(1,6,null),(2,9,null),(3,2,''),(4,null,'here'),(5,null,'i'),(6,null,'now'),(7,5,'c'),(8,null,'t');

        drop table if exists join_condition_table_2222;
        create table join_condition_table_2222 (
        col_int_undef_signed int/*agg_type_placeholder*/   ,
        col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
        pk int/*agg_type_placeholder*/
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into join_condition_table_2222(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,''),(1,null,''),(2,null,''),(3,0,null),(4,7,null),(5,9,'d'),(6,9,null),(7,null,null),(8,null,null),(9,null,''),(10,null,'are'),(11,null,'were'),(12,2,''),(13,null,'one'),(14,null,'ok'),(15,null,'your'),(16,null,''),(17,null,null),(18,4,''),(19,null,null),(20,null,null),(21,null,null),(22,3,''),(23,null,null),(24,8,''),(25,2,'I''m'),(26,null,'e'),(27,3,'will'),(28,null,null),(29,3,'q'),(30,1,null),(31,3,null),(32,null,'p'),(33,null,'but'),(34,null,'v'),(35,null,'the'),(36,1,''),(37,null,'t'),(38,1,''),(39,null,''),(40,null,''),(41,4,null),(42,null,'just'),(43,4,null),(44,null,null),(45,null,null),(46,8,null),(47,5,''),(48,2,null),(49,null,'n'),(50,null,'p'),(51,1,'we'),(52,3,''),(53,null,'about'),(54,0,''),(55,2,'be'),(56,0,''),(57,null,null),(58,null,null),(59,null,null),(60,9,'how'),(61,null,'in'),(62,null,'me'),(63,5,null),(64,null,'I''ll'),(65,null,'j'),(66,8,null),(67,6,'g'),(68,1,null),(69,7,'if'),(70,3,null),(71,null,'hey'),(72,null,'I''m'),(73,4,''),(74,null,''),(75,6,'y'),(76,1,''),(77,3,'f'),(78,null,'c'),(79,4,'x'),(80,null,''),(81,4,null),(82,null,''),(83,4,'r'),(84,null,''),(85,null,''),(86,null,null),(87,2,''),(88,null,'f'),(89,0,null),(90,9,'g'),(91,null,'I''ll'),(92,8,'p'),(93,5,'really'),(94,null,null),(95,0,'up'),(96,4,'k'),(97,null,'if'),(98,1,'f'),(99,1,'q');

        drop table if exists join_condition_table_3333;
        create table join_condition_table_3333 (
        col_int_undef_signed int/*agg_type_placeholder*/   ,
        col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
        pk int/*agg_type_placeholder*/
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into join_condition_table_3333(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,'my'),(1,null,'a'),(2,5,''),(3,0,'that'),(4,0,'want'),(5,null,'g'),(6,null,null),(7,null,''),(8,null,null),(9,3,'b'),(10,null,'her'),(11,6,''),(12,null,'k'),(13,null,'then'),(14,2,null),(15,null,''),(16,null,'g'),(17,null,'x'),(18,null,'d'),(19,null,null);

        drop table if exists join_condition_table_4444;
        create table join_condition_table_4444 (
        col_int_undef_signed int/*agg_type_placeholder*/   ,
        col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
        pk int/*agg_type_placeholder*/
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into join_condition_table_4444(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,3,'s'),(1,8,''),(2,null,null),(3,7,'k'),(4,null,'x'),(5,null,''),(6,null,'will'),(7,null,'so');
        """

    order_qt_join_with_add_min_max_rule """SELECT *  FROM join_condition_table_1111 AS t1
            LEFT ANTI JOIN join_condition_table_2222 AS t2 ON t2 . `pk` = 2 OR t2 . `pk` < 0
            RIGHT ANTI JOIN join_condition_table_1111 AS alias1 ON alias1 . `pk` >= 3
            CROSS JOIN join_condition_table_3333 AS alias2
            INNER JOIN join_condition_table_4444 AS alias3 ;
            """
}
