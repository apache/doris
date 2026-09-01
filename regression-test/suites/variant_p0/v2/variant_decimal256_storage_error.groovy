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

suite("variant_decimal256_storage_error", "p0,nonConcurrent") {
    sql "SET enable_decimal256 = true"
    sql "SET default_variant_enable_doc_mode = false"

    sql "DROP TABLE IF EXISTS variant_decimal256_materialized"
    sql """
        CREATE TABLE variant_decimal256_materialized (
            id INT,
            var VARIANT<
                'decimal256_': DECIMAL(76, 2),
                PROPERTIES(
                    "variant_max_subcolumns_count" = "1",
                    "variant_enable_typed_paths_to_sparse" = "false")
            >
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_decimal256_materialized VALUES
            (1, parse_to_variant('{"decimal256_":"123.45","hot":1}')),
            (2, parse_to_variant('{"hot":2}'))
    """
    sql "SYNC"
    test {
        sql "SELECT var FROM variant_decimal256_materialized ORDER BY id"
        exception "Conversion from Decimal256 materialized storage column to Variant V2 is not supported"
    }

    sql "DROP TABLE IF EXISTS variant_decimal256_sparse"
    sql """
        CREATE TABLE variant_decimal256_sparse (
            id INT,
            var VARIANT<
                'decimal256_': DECIMAL(76, 2),
                PROPERTIES(
                    "variant_max_subcolumns_count" = "1",
                    "variant_enable_typed_paths_to_sparse" = "true")
            >
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_decimal256_sparse VALUES
            (1, parse_to_variant('{"decimal256_":"123.45","hot":1}')),
            (2, parse_to_variant('{"hot":2}'))
    """
    sql "SYNC"
    test {
        sql "SELECT var FROM variant_decimal256_sparse ORDER BY id"
        exception "Conversion from Decimal256 storage cell to Variant V2 is not supported"
    }

    sql "DROP TABLE IF EXISTS variant_decimal256_array_materialized"
    sql """
        CREATE TABLE variant_decimal256_array_materialized (
            id INT,
            var VARIANT<
                'array_decimal256_': ARRAY<DECIMAL(76, 2)>,
                PROPERTIES(
                    "variant_max_subcolumns_count" = "1",
                    "variant_enable_typed_paths_to_sparse" = "false")
            >
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_decimal256_array_materialized VALUES
            (1, parse_to_variant('{"array_decimal256_":[1.23,2.34]}'))
    """
    sql "SYNC"
    test {
        sql "SELECT var FROM variant_decimal256_array_materialized ORDER BY id"
        exception "Conversion from Decimal256 materialized storage column to Variant V2 is not supported"
    }
}
