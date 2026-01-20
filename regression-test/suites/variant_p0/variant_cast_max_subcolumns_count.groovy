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

suite("variant_cast_max_subcolumns_count", "p0") {
    def t1 = "variant_cast_src"
    def t2 = "variant_cast_dst"
    def t3 = "variant_cast_typed_mismatch"
    def t4 = "variant_cast_sparse_shard_mismatch"

    sql "DROP TABLE IF EXISTS ${t1}"
    sql "DROP TABLE IF EXISTS ${t2}"
    sql "DROP TABLE IF EXISTS ${t3}"
    sql "DROP TABLE IF EXISTS ${t4}"

    sql """
        CREATE TABLE ${t1} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "1",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t2} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """INSERT INTO ${t1} VALUES (1, '{"a": 1, "b": 2}'), (2, '{"b": 3}')"""
    sql """
        INSERT INTO ${t2}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t1}
    """
    qt_sql_ok """SELECT v['a'], v['b'], * FROM ${t2} ORDER BY k"""

    sql """
        CREATE TABLE ${t3} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "true")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    test {
        sql """
            INSERT INTO ${t3}
            SELECT k,
                   CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                                "variant_enable_typed_paths_to_sparse" = "true")>)
            FROM ${t1}
        """
        exception "cannot cast"
    }

    sql """
        CREATE TABLE ${t4} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "false",
                                 "variant_sparse_hash_shard_count" = "3")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    test {
        sql """
            INSERT INTO ${t4}
            SELECT k,
                   CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                                "variant_enable_typed_paths_to_sparse" = "false",
                                                "variant_sparse_hash_shard_count" = "3")>)
            FROM ${t1}
        """
        exception "cannot cast"
    }
}
