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

/**
 * The FE local shuffle planner follows enable_local_exchange_before_streaming_agg the way BE
 * StreamingAggOperatorX::required_data_distribution() does (apache/doris#66222): by default a
 * streaming preaggregation keeps the distribution it inherits, and reshuffles by its grouping
 * keys only when the switch is on, or when its child breaks the grouping-key distribution (a
 * serial child, or one that spreads rows with a non-hash local exchange, such as the probe side
 * of a nested loop join or the input of a table function).
 *
 * Local exchanges only show up in EXPLAIN DISTRIBUTED PLAN, where the node printed right below a
 * STREAMING aggregate is its child.
 */
suite("test_local_exchange_before_streaming_agg", "p0") {
    sql "DROP TABLE IF EXISTS le_streaming_agg_t1"
    sql "DROP TABLE IF EXISTS le_streaming_agg_t2"

    sql """
        CREATE TABLE le_streaming_agg_t1 (
            k1 INT NOT NULL,
            k2 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 16
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE le_streaming_agg_t2 (
            k1 INT NOT NULL,
            k2 INT,
            v2 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 16
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO le_streaming_agg_t1 VALUES
        (1, 10, 1), (2, 20, 5), (3, 10, 9), (4, 30, 2),
        (5, 20, 7), (6, 10, 4), (7, 40, 8), (8, 30, 3)
    """

    sql """INSERT INTO le_streaming_agg_t2 VALUES
        (1, 10, 6), (2, 50, 2), (3, 20, 8), (4, 10, 1), (5, 60, 5)
    """

    // Pin the fuzzy variables that change the aggregation or exchange shape. Non-pooling scans of
    // 16 buckets stay parallel on every BE, so only the child under test can break the key
    // distribution.
    def hints = { String planner, String streamingAggLe -> """/*+SET_VAR(
        enable_local_shuffle_planner=${planner},
        enable_local_exchange_before_streaming_agg=${streamingAggLe},
        enable_local_shuffle=true,
        ignore_storage_data_distribution=false,
        parallel_pipeline_task_num=4,
        disable_streaming_preaggregations=false,
        enable_bucketed_hash_agg=false,
        use_serial_exchange=false,
        enable_local_exchange_before_agg=true,
        disable_join_reorder=true,
        enable_sql_cache=false
    )*/""" }

    def queries = [
        // A union never breaks the key distribution.
        union: { String h -> """SELECT ${h} k2, count(*), sum(v)
            FROM (SELECT k2, v1 AS v FROM le_streaming_agg_t1
                  UNION ALL
                  SELECT k2, v2 AS v FROM le_streaming_agg_t2) u
            GROUP BY k2""" },
        // The probe side of a nested loop join is ADAPTIVE_PASSTHROUGH.
        nlj: { String h -> """SELECT ${h} t1.k2, count(*), sum(t2.v2)
            FROM le_streaming_agg_t1 t1 JOIN le_streaming_agg_t2 t2 ON t1.v1 > t2.v2
            GROUP BY t1.k2""" },
        // A table function takes PASSTHROUGH input.
        explode: { String h -> """SELECT ${h} e, count(*)
            FROM le_streaming_agg_t1 LATERAL VIEW explode(array(k1, k2)) tmp AS e
            GROUP BY e""" },
    ]

    // The plan node right below each STREAMING aggregate, with the type of a local exchange.
    def belowStreamingAggs = { String plan ->
        def lines = plan.readLines()
        def below = []
        for (int i = 0; i < lines.size(); i++) {
            if (!(lines[i] =~ /\|\s+STREAMING\s*$/)) {
                continue
            }
            for (int j = i + 1; j < lines.size(); j++) {
                def node = lines[j] =~ /^[\s|]*(?:----)?\d+:(V[A-Za-z-]+)/
                if (node.find()) {
                    def name = node.group(1)
                    if (name == "VLOCAL-EXCHANGE" && j + 1 < lines.size()) {
                        def type = lines[j + 1] =~ /type: (\S+)/
                        name += type.find() ? ":" + type.group(1) : ""
                    }
                    below << name
                    break
                }
            }
        }
        return below
    }
    def localHash = "VLOCAL-EXCHANGE:LOCAL_EXECUTION_HASH_SHUFFLE"

    // Default: the streaming preagg sits right on the union, with no local exchange in between.
    explain {
        sql "distributed plan ${queries['union'](hints('true', 'false'))}"
        check { plan ->
            def below = belowStreamingAggs(plan)
            assertTrue(below.contains("VUNION") && !below.contains(localHash), "${below}\n${plan}")
        }
    }

    // The switch asks for the hash local exchange between them.
    explain {
        sql "distributed plan ${queries['union'](hints('true', 'true'))}"
        check { plan ->
            def below = belowStreamingAggs(plan)
            assertTrue(below.contains(localHash) && !below.contains("VUNION"), "${below}\n${plan}")
        }
    }

    // A child that breaks the key distribution still gets the hash local exchange by default.
    for (def name : ["nlj", "explode"]) {
        explain {
            sql "distributed plan ${queries[name](hints('true', 'false'))}"
            check { plan ->
                def below = belowStreamingAggs(plan)
                assertTrue(below.contains(localHash), "${name}: ${below}\n${plan}")
            }
        }
    }

    // Same results from the FE and BE local shuffle planners, with the switch off and on.
    for (def name : queries.keySet()) {
        for (def planner : ["true", "false"]) {
            for (def streamingAggLe : ["false", "true"]) {
                "order_qt_${name}_planner_${planner}_le_${streamingAggLe}"(
                        queries[name](hints(planner, streamingAggLe)))
            }
        }
    }
}
