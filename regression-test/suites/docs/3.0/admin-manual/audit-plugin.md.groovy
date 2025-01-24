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

suite("docs/3.0/admin-manual/audit-plugin.md") {
    try {
        multi_sql """
        create database doris_audit_db__;
        
        create table doris_audit_db__.doris_audit_log_tbl__
        (
            query_id varchar(48) comment "Unique query id",
            `time` datetime not null comment "Query start time",
            client_ip varchar(32) comment "Client IP",
            user varchar(64) comment "User name",
            db varchar(96) comment "Database of this query",
            state varchar(8) comment "Query result state. EOF, ERR, OK",
            error_code int comment "Error code of failing query.",
            error_message string comment "Error message of failing query.",
            query_time bigint comment "Query execution time in millisecond",
            scan_bytes bigint comment "Total scan bytes of this query",
            scan_rows bigint comment "Total scan rows of this query",
            return_rows bigint comment "Returned rows of this query",
            stmt_id int comment "An incremental id of statement",
            is_query tinyint comment "Is this statemt a query. 1 or 0",
            frontend_ip varchar(32) comment "Frontend ip of executing this statement",
            cpu_time_ms bigint comment "Total scan cpu time in millisecond of this query",
            sql_hash varchar(48) comment "Hash value for this query",
            sql_digest varchar(48) comment "Sql digest of this query, will be empty if not a slow query",
            peak_memory_bytes bigint comment "Peak memory bytes used on all backends of this query",
            stmt string comment "The original statement, trimed if longer than 2G"
        ) engine=OLAP
        duplicate key(query_id, `time`, client_ip)
        partition by range(`time`) ()
        distributed by hash(query_id) buckets 1
        properties(
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-30",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.enable" = "true",
            "replication_num" = "3"
        );
        
        create table doris_audit_db__.doris_slow_log_tbl__
        (
            query_id varchar(48) comment "Unique query id",
            `time` datetime not null comment "Query start time",
            client_ip varchar(32) comment "Client IP",
            user varchar(64) comment "User name",
            db varchar(96) comment "Database of this query",
            state varchar(8) comment "Query result state. EOF, ERR, OK",
            error_code int comment "Error code of failing query.",
            error_message string comment "Error message of failing query.",
            query_time bigint comment "Query execution time in millisecond",
            scan_bytes bigint comment "Total scan bytes of this query",
            scan_rows bigint comment "Total scan rows of this query",
            return_rows bigint comment "Returned rows of this query",
            stmt_id int comment "An incremental id of statement",
            is_query tinyint comment "Is this statemt a query. 1 or 0",
            frontend_ip varchar(32) comment "Frontend ip of executing this statement",
            cpu_time_ms bigint comment "Total scan cpu time in millisecond of this query",
            sql_hash varchar(48) comment "Hash value for this query",
            sql_digest varchar(48) comment "Sql digest of a slow query",
            peak_memory_bytes bigint comment "Peak memory bytes used on all backends of this query",
            stmt string comment "The original statement, trimed if longer than 2G "
        ) engine=OLAP
        duplicate key(query_id, `time`, client_ip)
        partition by range(`time`) ()
        distributed by hash(query_id) buckets 1
        properties(
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-30",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.enable" = "true",
            "replication_num" = "3"
        );
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/3.0/admin-manual/audit-plugin.md failed to exec, please fix it", t)
    }
}
