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

import org.junit.Assert;

suite("test_table_version") {
    def tableNameNum = "t_test_version_user_num"
    def dbName = "regression_test_table_p0"
    sql """drop table if exists `${tableNameNum}`"""

    sql """
        CREATE TABLE `${tableNameNum}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_3000 VALUES [('2017-03-01'), ('2017-04-01')),
        PARTITION p201704_all VALUES [('2017-04-01'), ('2017-05-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    def dbId = getDbId();
    def visibleVersion = getTableVersion(dbId,tableNameNum);
    assertEquals(1, visibleVersion);

    sql """
        insert into ${tableNameNum} values(1,"2017-01-15",1);
        """
    visibleVersion = getTableVersion(dbId,tableNameNum);
    assertEquals(2, visibleVersion);

    sql """
        insert into ${tableNameNum} values(1,"2017-03-15",1);
        """
    visibleVersion = getTableVersion(dbId,tableNameNum);
    assertEquals(3, visibleVersion);

    // drop an empty partition will not add table version
    sql """
        alter table ${tableNameNum} drop partition p201704_all force;
        """
    visibleVersion = getTableVersion(dbId,tableNameNum);
    assertEquals(3, visibleVersion);

    // drop an non-empty partition will add table version
    sql """
        alter table ${tableNameNum} drop partition p201703_3000 force;
        """
    visibleVersion = getTableVersion(dbId,tableNameNum);
    assertEquals(4, visibleVersion);

    sql """
        ALTER TABLE ${tableNameNum} ADD TEMPORARY PARTITION p201702_2000_1 VALUES [('2017-02-01'), ('2017-03-01'));
    """
    sql """
        ALTER TABLE ${tableNameNum} REPLACE PARTITION (p201702_2000) WITH TEMPORARY PARTITION (p201702_2000_1);
    """
    visibleVersion = getTableVersion(dbId,tableNameNum);
    assertEquals(5, visibleVersion);

    sql """drop table if exists `${tableNameNum}`"""
}
