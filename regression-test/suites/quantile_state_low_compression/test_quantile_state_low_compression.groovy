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
            SELECT quantile_state_to_base64(to_quantile_state(1.0, 128)) !=
                       quantile_state_to_base64(to_quantile_state(1.0, 2048)),
                   quantile_state_to_base64(quantile_state_from_base64(
                       quantile_state_to_base64(to_quantile_state(1.0, 128)))) =
                       quantile_state_to_base64(to_quantile_state(1.0, 128))
            FROM numbers("number" = "1")
        """

        // The fifth serialized byte is the representation tag: 02=EXPLICIT, 03=TDIGEST.
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
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 128)) AS s
                  FROM numbers("number" = "1440")) x
        """
        qt_2048_explicit """
            SELECT substr(hex(from_base64(quantile_state_to_base64(s))), 9, 2),
                   quantile_percent(s, 0), quantile_percent(s, 1)
            FROM (SELECT quantile_union(to_quantile_state(number, 2048)) AS s
                  FROM numbers("number" = "2048")) x
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
            SELECT 0, 1 + number DIV 480, quantile_union(to_quantile_state(7.0, 128))
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
    }
}
