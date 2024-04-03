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

suite("recover") {
    def table = "test_recover"

    // create table and insert data
    sql """ drop table if exists ${table} """
    sql """
    create table ${table} (
        `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    partition by range(da)(
        PARTITION p3 VALUES LESS THAN ('2023-01-01'),
        PARTITION p4 VALUES LESS THAN ('2024-01-01'),
        PARTITION p5 VALUES LESS THAN ('2025-01-01')
    )
    distributed by hash(id) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """

    sql """ insert into ${table} values(1, 'a', '2022-01-02'); """
    sql """ insert into ${table} values(2, 'a', '2023-01-02'); """
    sql """ insert into ${table} values(3, 'a', '2024-01-02'); """

    // drop partition
    sql """ ALTER TABLE ${table} DROP PARTITION p3; """

    // add partition with the same name as the dropped partition
    sql """ alter table ${table} add PARTITION p3 VALUES LESS THAN("2026-01-01"); """
    sql """ insert into ${table} values(4, 'a', '2025-01-02'); """
    sql """ insert into ${table} PARTITION(p3) values (5, 'a', '2025-01-02'); """

    // recover partition use new name
    sql """ recover partition p3 as p6 from regression_test_catalog_recycle_bin_p0.${table}; """

    // insert into partition p3
    sql """ insert into ${table} PARTITION(p3) values (6, 'a', '2025-01-02'); """
}
