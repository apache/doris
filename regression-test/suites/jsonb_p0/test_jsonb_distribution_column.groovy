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

suite("test_jsonb_distribution_column", "p0") {
    sql "DROP TABLE IF EXISTS test_jsonb_distribution_column"
    test {
        sql """
            CREATE TABLE test_jsonb_distribution_column (
                k INT,
                c JSONB
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(c) BUCKETS 4
            PROPERTIES("replication_num" = "1")
        """
        exception "JsonType type should not be used in distribution column[c]."
    }

    sql "DROP TABLE IF EXISTS test_variant_distribution_column"
    test {
        sql """
            CREATE TABLE test_variant_distribution_column (
                k INT,
                v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(v) BUCKETS 4
            PROPERTIES("replication_num" = "1")
        """
        exception "Variant type should not be used in distribution column[v]."
    }
}
