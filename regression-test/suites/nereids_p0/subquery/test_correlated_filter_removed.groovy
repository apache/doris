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

suite("test_correlated_filter_removed") {
    multi_sql """
        drop table if exists table_6_test_correlated_filter_removed;

        create table table_6_test_correlated_filter_removed (
        col_int_undef_signed int/*agg_type_placeholder*/   ,
        col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
        pk int/*agg_type_placeholder*/
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");

        insert into table_6_test_correlated_filter_removed(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,'think'),(1,null,''),(2,2,''),(3,null,'r'),(4,null,null),(5,8,'here');

        drop table if exists table_100_test_correlated_filter_removed;

        create table table_100_test_correlated_filter_removed (
        col_int_undef_signed int/*agg_type_placeholder*/   ,
        col_varchar_10__undef_signed varchar(10)/*agg_type_placeholder*/   ,
        pk int/*agg_type_placeholder*/
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");

        insert into table_100_test_correlated_filter_removed(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,''),(1,null,''),(2,null,''),(3,0,null),(4,7,null),(5,9,'d'),(6,9,null),(7,null,null),(8,null,null),(9,null,''),(10,null,'are'),(11,null,'were'),(12,2,''),(13,null,'one'),(14,null,'ok'),(15,null,'your'),(16,null,''),(17,null,null),(18,4,''),(19,null,null),(20,null,null),(21,null,null),(22,3,''),(23,null,null),(24,8,''),(25,2,'I''m'),(26,null,'e'),(27,3,'will'),(28,null,null),(29,3,'q'),(30,1,null),(31,3,null),(32,null,'p'),(33,null,'but'),(34,null,'v'),(35,null,'the'),(36,1,''),(37,null,'t'),(38,1,''),(39,null,''),(40,null,''),(41,4,null),(42,null,'just'),(43,4,null),(44,null,null),(45,null,null),(46,8,null),(47,5,''),(48,2,null),(49,null,'n'),(50,null,'p'),(51,1,'we'),(52,3,''),(53,null,'about'),(54,0,''),(55,2,'be'),(56,0,''),(57,null,null),(58,null,null),(59,null,null),(60,9,'how'),(61,null,'in'),(62,null,'me'),(63,5,null),(64,null,'I''ll'),(65,null,'j'),(66,8,null),(67,6,'g'),(68,1,null),(69,7,'if'),(70,3,null),(71,null,'hey'),(72,null,'I''m'),(73,4,''),(74,null,''),(75,6,'y'),(76,1,''),(77,3,'f'),(78,null,'c'),(79,4,'x'),(80,null,''),(81,4,null),(82,null,''),(83,4,'r'),(84,null,''),(85,null,''),(86,null,null),(87,2,''),(88,null,'f'),(89,0,null),(90,9,'g'),(91,null,'I''ll'),(92,8,'p'),(93,5,'really'),(94,null,null),(95,0,'up'),(96,4,'k'),(97,null,'if'),(98,1,'f'),(99,1,'q');
    """

    sql """
        SELECT *
        FROM table_6_test_correlated_filter_removed AS t1
        WHERE t1.`pk` + 2 IN 
            (SELECT 1
            FROM table_100_test_correlated_filter_removed AS t2
            WHERE t1.col_int_undef_signed is NULL
                    AND t1.col_int_undef_signed is NOT NULL) ;
    """
}
