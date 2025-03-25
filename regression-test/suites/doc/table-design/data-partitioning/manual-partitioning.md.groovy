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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/data-partitioning/manual-partitioning.md") {
    try {
        sql "drop table if exists null_list"
        multi_sql """
        create table null_list(
        k0 varchar null
        )
        partition by list (k0)
        (
        PARTITION pX values in ((NULL))
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1");
        insert into null_list values (null);
        select * from null_list;
        """

        sql "drop table if exists null_range"
        multi_sql """
        create table null_range(
        k0 int null
        )
        partition by range (k0)
        (
        PARTITION p10 values less than (10),
        PARTITION p100 values less than (100),
        PARTITION pMAX values less than (maxvalue)
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1");
        insert into null_range values (null);
        select * from null_range partition(p10);
        """

        sql "drop table if exists null_range2"
        sql """
        create table null_range2(
        k0 int null
        )
        partition by range (k0)
        (
        PARTITION p200 values [("100"), ("200"))
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1")
        """
        try {
            sql " insert into null_range2 values (null) "
            Assertions.fail("The SQL above should throw an exception as follows:\n\t\terrCode = 2, detailMessage = Insert has filtered data in strict mode. url: http://127.0.0.1:8040/api/_load_error_log?file=__shard_0/error_log_insert_stmt_b3a6d1f1fac74750-b3bb5d6e92a66da4_b3a6d1f1fac74750_b3bb5d6e92a66da4")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("errCode = 2, detailMessage = Insert has filtered data in strict mode. url:"))
        }
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/data-partitioning/manual-partitioning.md failed to exec, please fix it", t)
    }
}
