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

suite("mow_insert_with_partition_drop") {
    def table = "mow_insert_with_partition_drop"
    // create table and insert data
    sql """ drop table if exists ${table}"""
    sql """
    create table ${table} (
        `da` date,
        `id` int(11),
        `name` varchar(128)
    )
    engine=olap
    unique key(da)
    partition by range(da)(
        PARTITION p3 VALUES LESS THAN ('2023-01-01'),
        PARTITION p4 VALUES LESS THAN ('2024-01-01'),
        PARTITION p5 VALUES LESS THAN ('2025-01-01')
    )
    distributed by hash(da) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """
    def do_insert_into = {
        int j = 1
        while (j < 30) {
            try {
                logger.info("round=" + j)
                sql """ insert into ${table} values('2022-01-02', 2, 'a'); """
                sql """ insert into ${table} values('2023-01-02', 2, 'a'); """
                sql """ insert into ${table} values('2024-01-02', 3, 'a'); """
                j++
            } catch (Exception e) {
                logger.info("exception=" + e.getMessage())
                assertTrue(e.getMessage().contains("Insert has filtered data in strict mode. url:") ||
                        (e.getMessage().contains("partition") && e.getMessage().contains("does not exist")))
            }

        }
    }

    def t1 = Thread.startDaemon {
        do_insert_into()
    }
    for (int i = 0; i < 30; i++) {
        sql """ ALTER TABLE ${table} DROP PARTITION p3 force; """
        sql """ ALTER TABLE ${table} ADD PARTITION p3 VALUES LESS THAN ('2023-01-01'); """
    }
    t1.join()
}