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

suite("test_quantile_state_cow") {
    sql "DROP TABLE IF EXISTS test_quantile_state_cow"
    sql """
        CREATE TABLE test_quantile_state_cow (
            app INT NOT NULL,
            topic INT NOT NULL,
            chunk INT NOT NULL,
            duration_state QUANTILE_STATE NOT NULL
        ) DUPLICATE KEY(app, topic, chunk)
        DISTRIBUTED BY HASH(app, topic, chunk) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    // Each stored state contains 4096 inputs, so consumers receive TDIGEST
    // values rather than SINGLE/EXPLICIT states. Topics have disjoint values
    // to make contamination by the app-level aggregate observable.
    sql """
        INSERT INTO test_quantile_state_cow
        SELECT number % 4, (number DIV 4) % 2, (number DIV 8) % 8,
               quantile_union(to_quantile_state(10 + 100 * ((number DIV 4) % 2), 2048))
        FROM numbers("number" = "262144")
        GROUP BY 1, 2, 3
    """

    def query = """
        WITH filtered AS (
            SELECT * FROM test_quantile_state_cow
        ), grain AS (
            SELECT app, topic, count(*) AS chunks FROM filtered GROUP BY app, topic
        ), topics AS (
            SELECT app, topic, quantile_union(duration_state) AS q
            FROM filtered GROUP BY app, topic
        ), apps AS (
            SELECT app, quantile_union(duration_state) AS q
            FROM filtered GROUP BY app
        )
        SELECT g.app, g.topic, g.chunks,
               quantile_percent(t.q, 0), quantile_percent(t.q, 0.5),
               quantile_percent(t.q, 0.9), quantile_percent(t.q, 1),
               quantile_percent(a.q, 0), quantile_percent(a.q, 0.5),
               quantile_percent(a.q, 0.9), quantile_percent(a.q, 1)
        FROM grain g JOIN topics t ON g.app = t.app AND g.topic = t.topic
        JOIN apps a ON g.app = a.app
        ORDER BY g.app, g.topic
    """

    sql "SET enable_cte_materialize = false"
    def independentScans = sql(query)
    sql "SET enable_cte_materialize = true"
    sql "SET inline_cte_referenced_threshold = 0"
    explain {
        sql(query)
        contains "MultiCastDataSinks"
    }
    // Compare execution strategies, not handwritten approximate percentiles.
    // A small tolerance permits different legal TDigest merge orders.
    for (int iteration = 0; iteration < 10; ++iteration) {
        def sharedScan = sql(query)
        assertEquals(independentScans.size(), sharedScan.size())
        for (int row = 0; row < independentScans.size(); ++row) {
            for (int col = 0; col < 3; ++col) {
                assertEquals(independentScans[row][col], sharedScan[row][col])
            }
            for (int col = 3; col < independentScans[row].size(); ++col) {
                assertTrue(Math.abs((independentScans[row][col] as double)
                        - (sharedScan[row][col] as double)) < 1.0)
            }
        }
    }
}
