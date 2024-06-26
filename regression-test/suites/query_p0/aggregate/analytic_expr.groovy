/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("analytic_expr_test") {
    sql """drop table if exists test_load_03;"""
    sql """
        CREATE TABLE `test_load_03` (
          `id` varchar(10) NULL,
          `ctime` datetime NULL,
          `url` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        ); 
    """

    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Marry', '2015-11-12 03:14:30', 'url5');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Marry', '2015-11-12 02:13:00', 'url4');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Marry', '2015-11-12 01:16:40', 'url3');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Marry', '2015-11-12 01:15:10', 'url2');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Marry', '2015-11-12 01:10:00', 'url1');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Peter', '2015-10-12 03:14:30', 'url5');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Peter', '2015-10-12 02:13:00', 'url4');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Peter', '2015-10-12 01:16:40', 'url3');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Peter', '2015-10-12 01:15:10', 'url2');"""
    sql """INSERT INTO `test_load_03` (`id`, `ctime`, `url`) VALUES ('Peter', '2015-10-12 01:10:00', 'url1');"""

    sql """select
         id,url,ctime,
         first_value(ctime) over(partition by id order by ctime) as first_ctime,
            lag(ctime,1,0) over(partition by id order by ctime) as lag_ctime,
            lead(ctime,1,0) over(partition by id order by ctime) as lead_ctime
        from test_load_03; 
    """
}
