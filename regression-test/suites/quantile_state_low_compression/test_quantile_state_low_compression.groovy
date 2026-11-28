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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_quantile_state_low_compression", "docker,p1,quantile_state_low_compression") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.beConfigs += ["enable_low_compression_quantile_state=true"]
    docker(options) {
        qt_singleton_roundtrip """
            SELECT quantile_state_to_base64(to_quantile_state(1.0, 32)) !=
                       quantile_state_to_base64(to_quantile_state(1.0, 2048)),
                   quantile_state_to_base64(quantile_state_from_base64(
                       quantile_state_to_base64(to_quantile_state(1.0, 32)))) =
                       quantile_state_to_base64(to_quantile_state(1.0, 32))
            FROM numbers("number" = "1")
        """

        qt_nullable_const_compression """
            SELECT quantile_state_to_base64(to_quantile_state(1.0, nullable(cast(32 AS float)))) =
                   quantile_state_to_base64(to_quantile_state(1.0, 32))
            FROM numbers("number" = "1")
        """
        qt_nullable_129_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2)
            FROM (
                SELECT quantile_union(
                    to_quantile_state(number, nullable(cast(32 AS float)))) AS s
                FROM numbers("number" = "129")
            ) x
        """

        // The fifth serialized byte is the representation tag: 02=EXPLICIT, 03=TDIGEST.
        qt_32_explicit """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 32)) AS s
                  FROM numbers("number" = "32")) x
        """
        qt_33_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 32)) AS s
                  FROM numbers("number" = "33")) x
        """
        qt_128_explicit """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 128)) AS s
                  FROM numbers("number" = "128")) x
        """
        qt_129_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 128)) AS s
                  FROM numbers("number" = "129")) x
        """
        qt_1440_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 1, 8),
                   substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 32)) AS s
                  FROM numbers("number" = "1440")) x
        """
        qt_2048_explicit """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 2048)) AS s
                  FROM numbers("number" = "2048")) x
        """

        // Fractional C bounds the integer sample count without rounding up.
        qt_fractional_32_explicit """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 32.5)) AS s
                  FROM numbers("number" = "32")) x
        """
        qt_fractional_33_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 32.5)) AS s
                  FROM numbers("number" = "33")) x
        """
        qt_fractional_129_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 128.5)) AS s
                  FROM numbers("number" = "129")) x
        """
        qt_fractional_2048_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 2047.5)) AS s
                  FROM numbers("number" = "2048")) x
        """
        qt_max_compression_2048_explicit """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 10000)) AS s
                  FROM numbers("number" = "2048")) x
        """
        qt_max_compression_2049_digest """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 10000)) AS s
                  FROM numbers("number" = "2049")) x
        """

        sql "DROP TABLE IF EXISTS test_quantile_state_low_partials"
        sql """
            CREATE TABLE test_quantile_state_low_partials (
                layout_id INT NOT NULL,
                part INT NOT NULL,
                s QUANTILE_STATE QUANTILE_UNION NOT NULL
            ) AGGREGATE KEY(layout_id, part)
            DISTRIBUTED BY HASH(layout_id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql """
            INSERT INTO test_quantile_state_low_partials
            SELECT 0, 0, quantile_union(to_quantile_state(7.0, 2048))
            FROM numbers("number" = "480")
        """
        sql """
            INSERT INTO test_quantile_state_low_partials
            SELECT 0, 1 + number DIV 480, quantile_union(to_quantile_state(7.0, 32))
            FROM numbers("number" = "960") GROUP BY 2
        """
        qt_stored_partials """
            SELECT part, quantile_percent(s, 0), quantile_percent(s, 0.99),
                   quantile_percent(s, 1)
            FROM test_quantile_state_low_partials ORDER BY part
        """
        qt_stored_merge """
            SELECT quantile_percent(quantile_union(s), 0),
                   quantile_percent(quantile_union(s), 0.99),
                   quantile_percent(quantile_union(s), 1)
            FROM test_quantile_state_low_partials
        """
        qt_ordered_merge """
            SELECT part, substr(hex(from_base64(quantile_state_to_base64(running))), 9, 2),
                   quantile_percent(running, 0.99)
            FROM (
                SELECT part, quantile_union(s) OVER (
                    ORDER BY part ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
                FROM test_quantile_state_low_partials
            ) x ORDER BY part
        """
        qt_reverse_merge """
            SELECT part, substr(hex(from_base64(quantile_state_to_base64(running))), 9, 2),
                   quantile_percent(running, 0.99)
            FROM (
                SELECT part, quantile_union(s) OVER (
                    ORDER BY part DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
                FROM test_quantile_state_low_partials
            ) x ORDER BY part
        """

        sql "DROP TABLE IF EXISTS test_quantile_state_compaction_merge"
        sql """
            CREATE TABLE test_quantile_state_compaction_merge (
                k INT NOT NULL,
                s QUANTILE_STATE QUANTILE_UNION NOT NULL
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true")
        """
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (1, to_quantile_state(1.0, 32))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (1, to_quantile_state(2.0, 32))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (2, to_quantile_state(3.0, 32))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (3, to_quantile_state(4.0, 32))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (3, to_quantile_state(5.0, 128))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (4, to_quantile_state(6.0, 32))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (4, to_quantile_state(7.0, 2048))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (5, to_quantile_state(8.0, 32))"
        sql "INSERT INTO test_quantile_state_compaction_merge VALUES (5, to_quantile_state(9.0, 10000))"
        // A digest-only group must also retain C=32 when compaction rewrites its row.
        sql """
            INSERT INTO test_quantile_state_compaction_merge
            SELECT 6, quantile_union(to_quantile_state(number, 32))
            FROM numbers("number" = "1440")
        """
        // The first four bytes of the state are the little-endian FLOAT compression.
        order_qt_before_compaction """
            SELECT k, substr(hex(from_base64(quantile_state_to_base64(s))), 1, 8),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM test_quantile_state_compaction_merge ORDER BY k
        """
        trigger_and_wait_compaction("test_quantile_state_compaction_merge", "full")
        order_qt_after_compaction """
            SELECT k, substr(hex(from_base64(quantile_state_to_base64(s))), 1, 8),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM test_quantile_state_compaction_merge ORDER BY k
        """
    }
}
