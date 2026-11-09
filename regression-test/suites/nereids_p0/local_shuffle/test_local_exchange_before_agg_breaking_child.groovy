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
 * With enable_local_exchange_before_agg=false the FE local shuffle planner decides the local
 * exchange below the LOCAL phase of an aggregation the way BE does since apache/doris#63766: the
 * phase keeps the distribution it inherits only while its child keeps the grouping-key
 * distribution. After a child that breaks it (a serial child, or one that spreads rows with a
 * non-hash local exchange, such as the probe side of a nested loop join or the input of a table
 * function):
 *   - a LOCAL AggSink (streaming preaggregation off) still hashes its input by the grouping keys;
 *   - a streaming distinct preaggregation (a GROUP BY without aggregate functions) applies
 *     enable_distinct_streaming_agg_force_passthrough, as with the switch on.
 *
 * Local exchanges only show up in EXPLAIN DISTRIBUTED PLAN, where the node printed right below a
 * VAGGREGATE (update serialize) is the child of that LOCAL phase.
 */
suite("test_local_exchange_before_agg_breaking_child", "p0") {
    sql "DROP TABLE IF EXISTS le_agg_breaking_t1"
    sql "DROP TABLE IF EXISTS le_agg_breaking_t2"

    sql """
        CREATE TABLE le_agg_breaking_t1 (
            k1 INT NOT NULL,
            k2 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 16
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE le_agg_breaking_t2 (
            k1 INT NOT NULL,
            k2 INT,
            v2 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 16
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO le_agg_breaking_t1 VALUES
        (1, 10, 1), (2, 20, 5), (3, 10, 9), (4, 30, 2),
        (5, 20, 7), (6, 10, 4), (7, 40, 8), (8, 30, 3)
    """

    sql """INSERT INTO le_agg_breaking_t2 VALUES
        (1, 10, 6), (2, 50, 2), (3, 20, 8), (4, 10, 1), (5, 60, 5)
    """

    // Pin the fuzzy variables that change the aggregation or exchange shape. Non-pooling scans of
    // 16 buckets stay parallel on every BE, so only the child under test can break the key
    // distribution.
    def hints = { String planner, String leBeforeAgg, String disableStreaming -> """/*+SET_VAR(
        enable_local_shuffle_planner=${planner},
        enable_local_exchange_before_agg=${leBeforeAgg},
        disable_streaming_preaggregations=${disableStreaming},
        enable_local_shuffle=true,
        ignore_storage_data_distribution=false,
        parallel_pipeline_task_num=4,
        enable_bucketed_hash_agg=false,
        use_serial_exchange=false,
        enable_local_exchange_before_streaming_agg=false,
        enable_distinct_streaming_aggregation=true,
        enable_distinct_streaming_agg_force_passthrough=true,
        disable_join_reorder=true,
        enable_sql_cache=false
    )*/""" }

    // Each query with the disable_streaming_preaggregations value that gives the LOCAL phase under
    // test: an AggSink for the first three, a streaming distinct preaggregation for the last.
    def queries = [
        // The probe side of a nested loop join is ADAPTIVE_PASSTHROUGH.
        nlj: ["true", { String h -> """SELECT ${h} t1.k2, count(*), sum(t2.v2)
            FROM le_agg_breaking_t1 t1 JOIN le_agg_breaking_t2 t2 ON t1.v1 > t2.v2
            GROUP BY t1.k2""" }],
        // A table function takes PASSTHROUGH input.
        explode: ["true", { String h -> """SELECT ${h} e, count(*)
            FROM le_agg_breaking_t1 LATERAL VIEW explode(array(k1, k2)) tmp AS e
            GROUP BY e""" }],
        // A union keeps the key distribution.
        union: ["true", { String h -> """SELECT ${h} k2, count(*), sum(v)
            FROM (SELECT k2, v1 AS v FROM le_agg_breaking_t1
                  UNION ALL
                  SELECT k2, v2 AS v FROM le_agg_breaking_t2) u
            GROUP BY k2""" }],
        explode_distinct: ["false", { String h -> """SELECT ${h} e
            FROM le_agg_breaking_t1 LATERAL VIEW explode(array(k1, k2)) tmp AS e
            GROUP BY e""" }],
    ]
    def query = { String name, String planner, String leBeforeAgg ->
        def (disableStreaming, template) = queries[name]
        return template(hints(planner, leBeforeAgg, disableStreaming))
    }

    // The plan node right below each LOCAL aggregate, with the type of a local exchange.
    def belowLocalAggs = { String plan ->
        def lines = plan.readLines()
        def below = []
        for (int i = 0; i < lines.size(); i++) {
            if (!(lines[i] =~ /\d+:VAGGREGATE \(update serialize\)/)) {
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
    def passthrough = "VLOCAL-EXCHANGE:PASSTHROUGH"

    // A LOCAL AggSink still hashes after a child that breaks the key distribution.
    for (def name : ["nlj", "explode"]) {
        explain {
            sql "distributed plan ${query(name, 'true', 'false')}"
            check { plan ->
                def below = belowLocalAggs(plan)
                assertTrue(below.contains(localHash), "${name}: ${below}\n${plan}")
            }
        }
    }

    // It keeps the distribution of a child that does not break it.
    explain {
        sql "distributed plan ${query('union', 'true', 'false')}"
        check { plan ->
            def below = belowLocalAggs(plan)
            assertTrue(below.contains("VUNION") && !below.contains(localHash), "${below}\n${plan}")
        }
    }

    // A streaming distinct preaggregation above a table function, whose output distribution is
    // unknown, gets the PASSTHROUGH exchange of enable_distinct_streaming_agg_force_passthrough.
    explain {
        sql "distributed plan ${query('explode_distinct', 'true', 'false')}"
        check { plan ->
            def below = belowLocalAggs(plan)
            assertTrue(below.contains(passthrough), "${below}\n${plan}")
        }
    }

    // Same results from the FE and BE local shuffle planners, with the switch off and on.
    for (def name : queries.keySet()) {
        for (def planner : ["true", "false"]) {
            for (def leBeforeAgg : ["false", "true"]) {
                "order_qt_${name}_planner_${planner}_le_${leBeforeAgg}"(query(name, planner, leBeforeAgg))
            }
        }
    }
}
