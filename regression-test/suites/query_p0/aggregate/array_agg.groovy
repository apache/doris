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

suite("array_agg") {
    sql "DROP TABLE IF EXISTS `test_array_agg`;"
    sql "DROP TABLE IF EXISTS `test_array_agg_int`;"
    sql "DROP TABLE IF EXISTS `test_array_agg_decimal`;"
    sql """
       CREATE TABLE `test_array_agg` (
        `id` int(11) NOT NULL,
        `label_name` varchar(32) default null,
        `value_field` string default null,
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """  

    sql """
    CREATE TABLE `test_array_agg_int` (
        `id` int(11) NOT NULL,
        `label_name` varchar(32) default null,
        `value_field` string default null,
        `age` int(11) default null
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """  

    sql """
       CREATE TABLE `test_array_agg_decimal` (
        `id` int(11) NOT NULL,
        `label_name` varchar(32) default null,
        `value_field` string default null,
        `age` int(11) default null,
         `o_totalprice` DECIMAL(15, 2) default NULL,
         `label_name_not_null` varchar(32) not null
    )ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """  

    sql """
    insert into `test_array_agg` values
    (1, "alex",NULL),
    (1, "LB", "V1_2"),
    (1, "LC", "V1_3"),
    (2, "LA", "V2_1"),
    (2, "LB", "V2_2"),
    (2, "LC", "V2_3"),
    (3, "LA", "V3_1"),
    (3, NULL, NULL),
    (3, "LC", "V3_3"),
    (4, "LA", "V4_1"),
    (4, "LB", "V4_2"),
    (4, "LC", "V4_3"),
    (5, "LA", "V5_1"),
    (5, "LB", "V5_2"),
    (5, "LC", "V5_3"),
    (5, NULL, "V5_3"),
    (6, "LC", "V6_3"),
    (6, "LC", NULL),
    (6, "LC", "V6_3"),
    (6, "LC", NULL),
    (6, NULL, "V6_3"),
    (7, "LC", "V7_3"),
    (7, "LC", NULL),
    (7, "LC", "V7_3"),
    (7, "LC", NULL),
    (7, NULL, "V7_3");
    """  
    
    sql """
    insert into `test_array_agg_int` values
    (1, "alex",NULL,NULL),
    (1, "LB", "V1_2",1),
    (1, "LC", "V1_3",2),
    (2, "LA", "V2_1",4),
    (2, "LB", "V2_2",5),
    (2, "LC", "V2_3",5),
    (3, "LA", "V3_1",6),
    (3, NULL, NULL,6),
    (3, "LC", "V3_3",NULL),
    (4, "LA", "V4_1",5),
    (4, "LB", "V4_2",6),
    (4, "LC", "V4_3",6),
    (5, "LA", "V5_1",6),
    (5, "LB", "V5_2",5),
    (5, "LC", "V5_3",NULL),
    (6, "LC", "V6_3",NULL),
    (6, "LC", NULL,NULL),
    (6, "LC", "V6_3",NULL),
    (6, "LC", NULL,NULL),
    (6, NULL, "V6_3",NULL),
    (7, "LC", "V7_3",NULL),
    (7, "LC", NULL,NULL),
    (7, "LC", "V7_3",NULL),
    (7, "LC", NULL,NULL),
    (7, NULL, "V7_3",NULL);
    """  

    sql """
    insert into `test_array_agg_decimal` values
    (1, "alex",NULL,NULL,NULL,"alex"),
    (1, "LB", "V1_2",1,NULL,"alexxing"),
    (1, "LC", "V1_3",2,1.11,"alexcoco"),
    (2, "LA", "V2_1",4,1.23,"alex662"),
    (2, "LB", "",5,NULL,""),
    (2, "LC", "",5,1.21,"alexcoco1"),
    (3, "LA", "V3_1",6,1.21,"alexcoco2"),
    (3, NULL, NULL,6,1.23,"alexcoco3"),
    (3, "LC", "V3_3",NULL,1.24,"alexcoco662"),
    (4, "LA", "",5,1.22,"alexcoco662"),
    (4, "LB", "V4_2",6,NULL,"alexcoco662"),
    (4, "LC", "V4_3",6,1.22,"alexcoco662"),
    (5, "LA", "V5_1",6,NULL,"alexcoco662"),
    (5, "LB", "V5_2",5,NULL,"alexcoco662"),
    (5, "LC", "V5_3",NULL,NULL,"alexcoco662"),
    (7, "", NULL,NULL,NULL,"alexcoco1"),
    (8, "", NULL,0,NULL,"alexcoco2");
    """ 

    qt_sql1 """
    SELECT array_agg(`label_name`) FROM `test_array_agg` GROUP BY `id` order by id;
    """
    qt_sql2 """
    SELECT array_agg(label_name) FROM `test_array_agg` GROUP BY value_field order by value_field;
    """
    qt_sql3 """
    SELECT array_agg(`label_name`) FROM `test_array_agg`;
    """
    qt_sql4 """
    SELECT array_agg(`value_field`) FROM `test_array_agg`;
    """
    qt_sql5 """
    SELECT id, array_agg(age) FROM test_array_agg_int GROUP BY id order by id;
    """

    qt_sql6 """
    select array_agg(label_name) from test_array_agg_decimal where id=7;
    """

    qt_sql7 """
    select array_agg(label_name) from test_array_agg_decimal group by id order by id;
    """

    qt_sql8 """
    select array_agg(age) from test_array_agg_decimal where id=7;
    """

    qt_sql9 """
    select id,array_agg(o_totalprice) from test_array_agg_decimal group by id order by id;
    """

    sql "DROP TABLE `test_array_agg`"
    sql "DROP TABLE `test_array_agg_int`"
    sql "DROP TABLE `test_array_agg_decimal`"
}
