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

 suite("test_group_having_alias") {
    sql """DROP TABLE IF EXISTS `test_having_alias_tb`; """
    sql """DROP TABLE IF EXISTS `tb_holiday`;"""

    sql """DROP TABLE IF EXISTS `test_having_alias_tb`; """
    sql """ 
        CREATE TABLE `tb_holiday` (
        `date` bigint(20) NOT NULL ,
        `holiday` tinyint(4) NOT NULL ,
        `holiday_cn` varchar(9) NOT NULL 
        ) ENGINE=OLAP
        UNIQUE KEY(`date`)
        DISTRIBUTED BY HASH(`date`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );
    """

    sql """
        insert into tb_holiday values (20221111, 1, 1 ),(20221112, 1, 1 ),(20221113, 1, 1 ),(20221116, 2, 2 ),(20221117, 2, 2 ),(20221118, 2, 2 );
    """

    qt_sql """
        SELECT
        date_format(date, '%x%v') AS `date`,
        count(date) AS `diff_days`
        FROM `tb_holiday`
        WHERE `date` between 20221111 AND 20221116
        GROUP BY date
        HAVING date = 20221111
        ORDER BY date;
    """

    qt_sql """
        SELECT
        date_format(date, '%x%v') AS `date2`,
        count(date) AS `diff_days`
        FROM `tb_holiday`
        WHERE `date` between 20221111 AND 20221116
        GROUP BY date2
        HAVING date2 = 202245
        ORDER BY date2;
    """

    qt_sql """
        SELECT
        date_format(date, '%x%v') AS `date`
        FROM `tb_holiday`
        WHERE `date` between 20221111 AND 20221116
        HAVING date = 202245
        ORDER BY date;
    """

    qt_sql """
        SELECT
        date_format(date, '%x%v') AS `date2`,
        count(date) AS `diff_days`
        FROM `tb_holiday`
        WHERE `date` between 20221111 AND 20221116
        GROUP BY date2
        HAVING date2 = 202245
        ORDER BY date2;
    """

    qt_sql """
        SELECT
        date_format(date, '%x%v') AS `date`
        FROM `tb_holiday`
        WHERE `date` between 20221111 AND 20221116
        HAVING date = 202245
        ORDER BY date;
    """

    sql """
         CREATE TABLE `test_having_alias_tb` (
          `id` int(11) NULL,
          `v1` bigint(20) NULL,
          `v2` bigint(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """
    sql """ INSERT INTO test_having_alias_tb values(1,1,1),(2,2,2),(2,3,3); """
    qt_case1 """ SELECT id, sum(v1) v1 FROM test_having_alias_tb GROUP BY id,v1 having(v1>1) ORDER BY id,v1; """
    qt_case2 """ SELECT id, sum(v1) v1, sum(v2) v2 FROM test_having_alias_tb GROUP BY id,v1 having(v1!=2 AND sum(v2)>1) ORDER BY id,v1; """
    qt_case3 """ SELECT id, v1-2 as v, sum(v2) v2 FROM test_having_alias_tb GROUP BY id,v having(v>0 AND sum(v2)>1) ORDER BY id,v; """
    qt_case4 """ SELECT id, v1-2 as v, sum(v2) vsum FROM test_having_alias_tb GROUP BY id,v having(v>0 AND vsum>1) ORDER BY id,v; """
    qt_case5 """ SELECT id, max(v1) v1 FROM test_having_alias_tb GROUP BY 1 having count(distinct v1)>1 ORDER BY id; """
    qt_case_cir2273 """ select sum( id * 0.5 ) val from test_having_alias_tb having val > 0; """

    qt_case6 """
      SELECT date_format(date, '%x%v') AS `date` FROM `tb_holiday` WHERE `date` between 20221111 AND 20221116 HAVING date = 202245 ORDER BY date;
    """
    sql """DROP TABLE IF EXISTS `test_having_alias_tb`; """
    sql """DROP TABLE IF EXISTS `tb_holiday`;"""

    sql """DROP TABLE IF EXISTS `test_having_alias_tb`; """
 }
