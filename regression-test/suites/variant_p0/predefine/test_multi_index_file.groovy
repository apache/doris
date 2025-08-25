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

suite("test_variant_multi_index_file", "p0, nonConcurrent"){ 
    sql """ set describe_extend_variant_column = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql "set default_variant_max_subcolumns_count = 100"

    def tableName = "test_variant_multi_index_nonCurrent"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<properties("variant_enable_typed_paths_to_sparse" = "false")> NOT NULL,
        INDEX idx_a_d (var) USING INVERTED PROPERTIES("parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_a_d_2 (var) USING INVERTED
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    sql """insert into ${tableName} values(1, '{"string" : "hello", "int" : 1, "array_string" : ["hello"]}'), (2, '{"string" : "world", "int" : 2, "array_string" : ["world"]}'), (3, '{"string" : "hello", "int" : 3, "array_string" : ["hello"]}'), (4, '{"string" : "world", "int" : 4, "array_string" : ["world"]}'), (5, '{"string" : "hello", "int" : 5, "array_string" : ["hello"]}') """


    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    String tablet_id = tablets[0].TabletId
    String backend_id = tablets[0].BackendId
    String ip = backendId_to_backendIP.get(backend_id)
    String port = backendId_to_backendHttpPort.get(backend_id)
    check_nested_index_file(ip, port, tablet_id, 2, 4, "V2")
}