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

suite("test_audit_log_behavior") {
    sql "drop table if exists audit_log_behavior"
    sql """
        CREATE TABLE `audit_log_behavior` (
          `id` bigint,
          `name` varchar(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        )
        """

    sql "set global enable_audit_plugin = true"
    sql "set global audit_plugin_max_sql_length = 58"
    sql "set global audit_plugin_max_batch_interval_sec = 1"
    sql "truncate table  __internal_schema.audit_log"

    int cnt = 0
    def sqls = [
            [
                    "insert into audit_log_behavior values (1, '3F6B9A_${cnt}')",
                    "insert into audit_log_behavior values (1, '3F6B9A_${cnt++}')"
            ],
            [
                    "insert into audit_log_behavior values (1, '3F6B9A_${cnt}'), (2, 'Jelly')",
                    "insert into audit_log_behavior values (1, '3F6B9A_${cnt++}'), (2, ... /* total 2 rows */"
            ],
            [
                    "insert into audit_log_behavior values (1, '3F6B9A_${cnt}'), (2, 'Jelly'), (3, 'foobar')",
                    "insert into audit_log_behavior values (1, '3F6B9A_${cnt++}'), (2, ... /* total 3 rows */"
            ],
            [
                    "insert into audit_log_behavior select 1, '3F6B9A_${cnt}'",
                    "insert into audit_log_behavior select 1, '3F6B9A_${cnt++}'"],
            [
                    "insert into audit_log_behavior select 1, '3F6B9A_${cnt}' union select 2, 'Jelly'",
                    "insert into audit_log_behavior select 1, '3F6B9A_${cnt++}' union  ..."
            ],
            [
                    "insert into audit_log_behavior select 1, '3F6B9A_${cnt}' from audit_log_behavior",
                    "insert into audit_log_behavior select 1, '3F6B9A_${cnt++}' from a ..."
            ],
            [
                    "select id, name from audit_log_behavior as loooooooooooooooong_alias",
                    "select id, name from audit_log_behavior as loooooooooooooo ..."
            ]
    ]

    // run queries
    for(int i = 0; i < cnt; i++) {
        def tuple2 = sqls.get(i)
        sql tuple2[0]
    }

    // check result
    for(int i = 0; i < cnt; i++) {
        def tuple2 = sqls.get(i)
        def res = sql "select stmt from __internal_schema.audit_log where stmt like '%3F6B9A_${i}%' order by time asc limit 1"
        while (res.isEmpty()) {
            sleep(1000)
            res = sql "select stmt from __internal_schema.audit_log where stmt like '%3F6B9A_${i}%' order by time asc limit 1"
        }
        assertEquals(res[0][0].toString(), tuple2[1].toString())
    }
    sql "set global enable_audit_plugin = false"
}
