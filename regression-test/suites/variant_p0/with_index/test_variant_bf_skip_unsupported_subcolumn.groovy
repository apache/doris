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

suite("test_variant_bf_skip_unsupported_subcolumn", "p0") {
    def tableName = "variant_bf_skip_unsupported_subcolumn"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            k bigint,
            v variant<'m.d' : double, properties(
                "variant_max_subcolumns_count" = "0",
                "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "bloom_filter_columns" = "v",
            "storage_format" = "V3"
        );
    """
    sql """INSERT INTO ${tableName} VALUES (1, '{"m":{"d":1.23}}')"""
    sql """INSERT INTO ${tableName} VALUES (2, '{"m":{"d":4.56}}')"""
    qt_sql """select cast(v['m']['d'] as double) from ${tableName} order by k"""
}
