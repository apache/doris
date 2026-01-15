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

suite("test_variant_field_pattern_invalid_inverted_index", "p0") {
    def tableName = "test_variant_field_pattern_invalid_inverted_index"

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'settings.enabled*' : boolean
                > NULL,
                INDEX idx_enabled (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "settings.enabled*",
                    "analyzer" = "unicode"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("field pattern: settings.enabled* is not supported for analyzed inverted index")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'settings.tags' : array<string>
                > NULL,
                INDEX idx_tags (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "settings.tags",
                    "normalizer" = "lowercase"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("field pattern: settings.tags is not supported for analyzed inverted index")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'metrics.count' : int
                > NULL,
                INDEX idx_count (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "metrics.count",
                    "parser" = "english"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("field pattern: metrics.count is not supported for analyzed inverted index")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'metrics.score' : int
                > NULL,
                INDEX idx_score (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "metrics.score",
                    "dict_compression" = "true"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("field pattern: metrics.score is not supported for dict_compression")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'metrics.price' : decimal(10, 2)
                > NULL,
                INDEX idx_price (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "metrics.price",
                    "analyzer" = "standard"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("field pattern: metrics.price is not supported for analyzed inverted index")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'metrics.day' : date
                > NULL,
                INDEX idx_day (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "metrics.day",
                    "parser" = "english"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("field pattern: metrics.day is not supported for analyzed inverted index")
    }
}
