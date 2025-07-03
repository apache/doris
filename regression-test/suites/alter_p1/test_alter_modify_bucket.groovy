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

suite('test_alter_modify_bucket') {
    def tbl = 'test_alter_modify_bucket_tbl'
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl} (
            `id1` VARCHAR(255) NULL COMMENT 'id1',
            `id2` VARCHAR(255) NULL COMMENT 'id2',
            `dt` VARCHAR(255) NOT NULL COMMENT '日期分区',
            `hr` VARCHAR(255) NOT NULL COMMENT '小时分区',
            `event_time` VARCHAR(255) NULL COMMENT '事件时间',
            `event_date` VARCHAR(255) NULL COMMENT '事件日期',
            `event_ts` VARCHAR(256) NULL COMMENT '事件发生时间戳(毫秒)'
        ) ENGINE=OLAP
        UNIQUE KEY(`id1`,`id2`,`dt`,`hr`)
        COMMENT 'xxx' 
        PARTITION BY LIST(`dt`, `hr`) (
            PARTITION p2024082021 VALUES IN (("2024-08-20", "21"))
        )
        DISTRIBUTED BY HASH(`id1`,`id2`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // check existed partition buckets
    List<List<Object>> res = sql """
        SHOW PARTITIONS FROM ${tbl}
    """
    assertEquals(res[0][8],"1")

    // add new partition and insert values
    sql """
        ALTER TABLE ${tbl} ADD PARTITION IF NOT EXISTS p2024082022 VALUES IN (("2024-08-20", "22"))
    """
    sql """
        INSERT INTO ${tbl} VALUES("1","1","2024-08-20","21","21:00:00","2024-08-20","1724158800000")
    """
    sql """
        INSERT INTO ${tbl} VALUES("2","2","2024-08-20","22","22:00:00","2024-08-20","1724162400000")
    """

    // modify table default buckets
    sql """
        ALTER TABLE ${tbl} MODIFY DISTRIBUTION DISTRIBUTED BY HASH(`id1`,`id2`) BUCKETS 2
    """

    // add new partition after modify tablet default buckets and insert values
    sql """
        ALTER TABLE ${tbl} ADD PARTITION IF NOT EXISTS p2024082023 VALUES IN (("2024-08-20", "23"))
    """
    sql """
        INSERT INTO ${tbl} VALUES("3","3","2024-08-20","23","23:00:00","2024-08-20","1724166000000")
    """

    // check all insert values
    qt_select """
        SELECT * FROM ${tbl} ORDER BY id1,id2
    """

    // check all partition buckets
    List<List<Object>> res1 = sql """
        SHOW PARTITIONS FROM ${tbl} ORDER BY `PartitionName`
    """
    assertEquals(res1[0][8],"1")
    assertEquals(res1[1][8],"1")
    assertEquals(res1[2][8],"2")

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
