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

suite("operative_slots") {
    sql """
    drop table if exists vt;
    CREATE TABLE IF NOT EXISTS vt (
                `user_id` int NOT NULL COMMENT "用户id",
                `name` STRING COMMENT "用户年龄",
                `v` VARIANT NULL
                )
                DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");

    insert into vt values (6, 'doris6', '{"k1" : 100, "k2": 1}'), (7, 'doris7', '{"k1" : 2, "k2": 2}');

    drop table if exists t;
    CREATE TABLE `t` (
    `k` int NULL,
    `v1` bigint NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );

    insert into t values (1, 1);

    set disable_join_reorder = true;
    """

    qt_explain "explain physical plan select * from t join[broadcast] vt on t.k = vt.v['k1'];"
}
