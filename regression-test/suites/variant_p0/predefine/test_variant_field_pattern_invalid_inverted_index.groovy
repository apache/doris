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

    sql " set default_variant_enable_doc_mode = true"

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
        exception("invalid INVERTED index: field pattern: settings.enabled*")
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
        exception("invalid INVERTED index: field pattern: settings.tags")
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
        exception("invalid INVERTED index: field pattern: metrics.count")
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
        exception("invalid INVERTED index: field pattern: metrics.score")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'metrics.price' : double
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
        exception("invalid INVERTED index: field pattern: metrics.price")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'metrics.day' : double
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
        exception("invalid INVERTED index: field pattern: metrics.day")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'settings.tags_parser' : array<string>
                > NULL,
                INDEX idx_tags_parser (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "settings.tags_parser",
                    "parser" = "english"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("INVERTED index with parser: english is not supported for array column")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'labels.name' : string
                > NULL,
                INDEX idx_name (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "labels.name",
                    "analyzer" = "unicode",
                    "parser" = "english"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("Cannot specify more than one of 'analyzer', 'parser', or 'normalizer' properties.")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'labels.desc' : string
                > NULL,
                INDEX idx_desc (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "labels.desc",
                    "parser" = "unknown"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("Invalid inverted index 'parser' value: unknown")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'labels.detail' : string
                > NULL,
                INDEX idx_detail (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "labels.detail",
                    "parser" = "english",
                    "parser_mode" = "fine_grained"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("parser_mode is only available for chinese and ik parser")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    test {
        sql """
            CREATE TABLE ${tableName} (
                `id` bigint NULL,
                `var` variant<
                    MATCH_NAME 'labels.code' : string
                > NULL,
                INDEX idx_code (var) USING INVERTED PROPERTIES(
                    "field_pattern" = "labels.code",
                    "dict_compression" = "maybe"
                ) COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception("Invalid inverted index 'dict_compression' value: maybe")
    }
}
