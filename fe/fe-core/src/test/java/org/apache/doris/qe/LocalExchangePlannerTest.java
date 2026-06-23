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

package org.apache.doris.qe;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.planner.AddLocalExchange;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.PlanShape;
import org.apache.doris.planner.PlanShapeDsl;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class LocalExchangePlannerTest extends TestWithFeService implements PlanShapeDsl {
    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE test.t1 (k1 INT, k2 INT, v1 INT) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 6 "
                + "PROPERTIES ('replication_num'='1')");
        createTable("CREATE TABLE test.t2 (k1 INT, k2 INT, v2 INT) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 6 "
                + "PROPERTIES ('replication_num'='1')");
    }

    // ---- helpers ----

    /**
     * Apply the default local-shuffle session config and let the caller tweak it.
     * Mirrors Trino's {@code Session.builder(defaultSession).setSystemProperty(...).build()}
     * pattern — tests only express what differs from the default, not the full set.
     */
    protected void setupLocalShuffleSession(java.util.function.Consumer<SessionVariable> tweaks)
            throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableLocalShufflePlanner(true);
        sv.setEnableLocalShuffle(true);
        sv.setEnableNereidsDistributePlanner(true);
        sv.setIgnoreStorageDataDistribution(true);
        sv.setPipelineTaskNum("4");
        sv.setForceToLocalShuffle(false);
        if (tweaks != null) {
            tweaks.accept(sv);
        }
    }

    /**
     * Run the SQL through the planner and assert the resulting distributed plan
     * matches {@code shape} in at least one fragment.  Replaces the boilerplate
     * {@code executeNereidsSql + cast planner + collect fragments + manual asserts}
     * with a one-liner, in the spirit of Trino's
     * {@code assertDistributedPlan(sql, pattern)}.
     */
    protected void assertPlanShape(String sql, PlanShape<?> shape) throws Exception {
        StmtExecutor executor = executeNereidsSql("explain distributed plan " + sql);
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        assertMatchesAnyFragment(planner.getFragments(), shape);
    }

    /**
     * Debug-only helper: run the SQL and dump every fragment's plan tree to stderr.
     * Use this when crafting a new {@link #assertPlanShape} assertion to see what
     * the real plan looks like, then replace this call with the proper DSL pattern.
     */
    protected void dumpPlan(String sql) throws Exception {
        StmtExecutor executor = executeNereidsSql("explain distributed plan " + sql);
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        System.err.println("=== dump for SQL: " + sql + " ===");
        printFragmentPlans(planner.getFragments());
    }

    /**
     * Assert that NO local exchange of the given type appears anywhere in any
     * fragment's plan tree.  Companion to {@link #assertPlanShape} for negative
     * checks where pinning the full shape would be brittle.
     */
    protected void assertNoLocalExchangeOfType(String sql, LocalExchangeType excludedType) throws Exception {
        StmtExecutor executor = executeNereidsSql("explain distributed plan " + sql);
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());
        Assertions.assertFalse(types.contains(excludedType),
                "expected no " + excludedType + " in plan, actual: " + types);
    }

    @Test
    public void testAggFromScanShapeDsl() throws Exception {
        // Same scenario as testAggFromScanUsesLocalExecutionHashShuffle but uses the
        // PlanShape DSL to assert a *structural* relationship rather than just
        // "explain contains the string LOCAL_EXECUTION_HASH_SHUFFLE somewhere".
        //
        // With ignore_storage_data_distribution=true the scan is serial (pooling
        // mode); AggSink requires HASH; framework's heavy-op fan-out kicks in and
        // inserts a PASSTHROUGH LE *before* the HASH LE so the 1-task scan does
        // not bottleneck the hash redistribution.  The full chain is:
        //   AggregationNode → LE(LOCAL_HASH) → LE(PASSTHROUGH) → OlapScan(t1)
        //
        // A substring check on explain text could not distinguish this from a
        // plan that only has the LOCAL_HASH LE — DSL pins the structure.
        setupLocalShuffleSession(null);
        assertPlanShape("select k1, k2, count(*) from test.t1 group by k1, k2",
                anyTree(
                        agg(
                                localExchange(LOCAL_HASH,
                                        localExchange(PT,
                                                olapScan("t1"))))));
    }

    @Test
    public void testAggWithoutKeyTwoPhase() throws Exception {
        // doc rule "Agg / 没有 groupby key" → PASSTHROUGH.
        // count(*) generates a two-phase aggregation:
        //   FINAL AggregationNode → ExchangeNode (UNPARTITIONED, serial)
        //                         → PARTIAL AggregationNode (serial, no keys)
        //                         → LE(PASSTHROUGH) (fan-out from 1-task pooling scan)
        //                         → OlapScan(t1)
        // The intermediate LE(PT) is the heavy-op fan-out for the pooling scan;
        // the serial PARTIAL agg sits right above it.
        setupLocalShuffleSession(null);
        assertPlanShape("select count(*) from test.t1",
                anyTree(
                        agg(
                                anyTree(
                                        agg(
                                                localExchange(PT,
                                                        olapScan("t1")))))));
    }

    @Test
    public void testBroadcastJoinPoolingShapeDsl() throws Exception {
        // doc rule "HashJoin / BROADCAST / 池化":
        //   probe ← LE(PASSTHROUGH) ← scan
        //   build ← LE(PASS_TO_ONE) ← Exchange (cross-fragment broadcast)
        // Matches doc Example 2 plus the pooling variant (PASS_TO_ONE instead of NOOP
        // on the serial build-side exchange).
        setupLocalShuffleSession(null);
        assertPlanShape("select * from test.t1 a join [broadcast] test.t2 b on a.k1=b.k1",
                anyTree(
                        hashJoin(
                                localExchange(PT,
                                        olapScan()),
                                localExchange(PASS_TO_ONE_LE,
                                        anyTree(exchange())))));
    }

    @Test
    public void testNlJoinPoolingShapeDsl() throws Exception {
        // doc rule "NL join / 池化": build BROADCAST, probe ADAPTIVE_PASSTHROUGH.
        // With pooling, the probe-side scan is serial (1 task) so the framework
        // additionally inserts an inner LE(PASSTHROUGH) below the ADAPTIVE_PASSTHROUGH
        // to fan the serial scan out before adaptive redistribution.  The build side
        // comes through a cross-fragment Exchange (broadcast).
        setupLocalShuffleSession(null);
        assertPlanShape("select * from test.t1 a, test.t2 b where a.k1 > b.k1",
                anyTree(
                        nestedLoopJoin(
                                localExchange(ADAPTIVE_PT,
                                        localExchange(PT,
                                                anyTree(olapScan()))),
                                localExchange(BROADCAST_LE,
                                        anyTree(exchange())))));
    }

    @Test
    public void testNullAwareLeftAntiJoinHasNoLocalExchange() throws Exception {
        // doc rule "HashJoin / NULL_AWARE_LEFT_ANTI_JOIN": NOOP/NOOP/NOOP — no LE
        // inserted on either side, in either direction.
        //   HashJoin(NULL_AWARE_LEFT_ANTI_JOIN)
        //     ← OlapScan(t1)             (probe, no LE)
        //     ← Exchange ← OlapScan(t2)  (build, no LE)
        setupLocalShuffleSession(null);
        assertPlanShape("select k1 from test.t1 where k1 not in (select k1 from test.t2)",
                anyTree(
                        hashJoin(
                                olapScan(),
                                anyTree(exchange()))
                                .where(j -> j.getJoinOp()
                                        == org.apache.doris.analysis.JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN)));
    }

    @Test
    public void testAnalyticNoPartitionByHasNoLocalExchange() throws Exception {
        // doc rule "Analytic / 无 partition by": serial path with PASSTHROUGH require
        // upstream.  Because the analytic fragment uses a cross-fragment UNPARTITIONED
        // Exchange to gather data into a single instance, no LE is inserted between
        // the AnalyticEvalNode and the Exchange — the Exchange already serves as the
        // serial source.
        setupLocalShuffleSession(null);
        assertPlanShape("select k1, row_number() over () from test.t1",
                anyTree(
                        analytic(
                                anyTree(exchange()))));
    }

    // -- Tier A: scenarios borrowed from Trino's TestAddExchangesPlans --

    @Test
    public void testUnionDistinctTwoPhaseAgg() throws Exception {
        // Borrowed from Trino's testRepartitionForUnionWithAnyTableScans.
        // UNION (not UNION ALL) implies DISTINCT — Doris realises that with a
        // two-phase Aggregation above the UnionNode.  The cross-fragment Exchanges
        // feeding the Union pre-shuffle the scan outputs; the LE(PT) above the
        // Union is the heavy-op fan-out for the partial agg over the gathered union.
        //   FINAL Agg ← Exchange ← PARTIAL Agg ← LE(PT) ← Union ← {Exchange ← scan} x 2
        setupLocalShuffleSession(null);
        assertPlanShape("select k1 from test.t1 union select k1 from test.t2",
                anyTree(
                        agg(
                                anyTree(
                                        agg(
                                                localExchange(PT,
                                                        union(
                                                                anyTree(olapScan()),
                                                                anyTree(olapScan()))))))));
    }

    @Test
    public void testUnionAllBeforeHashJoin() throws Exception {
        // Borrowed from Trino's testRepartitionForUnionAllBeforeHashJoin.
        // UNION ALL feeds into a hash join — the join's hash requirement is
        // satisfied by the cross-fragment Exchanges sitting under each Union branch
        // (data is already hash-distributed by the time it reaches Union), so no
        // intra-fragment LE is needed on either probe or build side.
        setupLocalShuffleSession(null);
        assertPlanShape("select * from (select k1 from test.t1 union all select k1 from test.t2) u "
                        + "join test.t1 t3 on u.k1=t3.k1",
                anyTree(
                        hashJoin(
                                union(
                                        anyTree(olapScan()),
                                        anyTree(olapScan())),
                                anyTree(exchange()))));
    }

    @Test
    public void testWindowPartitionByBucketKey() throws Exception {
        // Borrowed from Trino's testWindowIsExactlyPartitioned.
        // PARTITION BY uses the table's bucket key (k1) — Doris's analytic eval
        // is colocate-eligible.  With pooling, the chain is:
        //   AnalyticEval ← Sort ← LE(LOCAL_HASH) ← LE(PT) ← scan
        // The inner LE(PT) is the heavy-op fan-out for the serial pooling scan.
        setupLocalShuffleSession(null);
        assertPlanShape("select k1, row_number() over (partition by k1) from test.t1",
                anyTree(
                        analytic(
                                sort(
                                        localExchange(LOCAL_HASH,
                                                localExchange(PT,
                                                        olapScan("t1")))))));
    }

    @Test
    public void testWindowPartitionByNonBucketKey() throws Exception {
        // Borrowed from Trino's testRowNumberIsExactlyPartitioned (negative variant).
        // PARTITION BY uses a non-bucket key (k2) — colocate is not eligible, so
        // the analytic eval lives in its own fragment fed by a cross-fragment
        // hash-partitioned Exchange.  No intra-fragment LE inside the analytic fragment.
        setupLocalShuffleSession(null);
        assertPlanShape("select k1, row_number() over (partition by k2) from test.t1",
                anyTree(
                        analytic(
                                sort(
                                        anyTree(exchange())))));
    }

    @Test
    public void testNestedUnionAll() throws Exception {
        // Borrowed from Trino's testNestedUnionAll.
        // Three-way UNION ALL flattens into a single UnionNode with three
        // cross-fragment Exchange children.  No LE since there's no downstream
        // consumer requiring hash distribution.
        setupLocalShuffleSession(null);
        assertPlanShape(
                "select k1 from test.t1 union all "
                        + "(select k1 from test.t2 union all select k1 from test.t1)",
                anyTree(
                        union(
                                anyTree(olapScan()),
                                anyTree(olapScan()),
                                anyTree(olapScan()))));
    }

    @Test
    public void testGroupedAggOverNlj() throws Exception {
        // Borrowed from Trino's testGroupedAggregationAboveUnionAllCrossJoined
        // (NLJ + agg variant).  NLJ output is ADAPTIVE_PASSTHROUGH; the outer Agg
        // requires HASH on k1.  Because ADAPTIVE_PASSTHROUGH does not satisfy HASH,
        // an LE(LOCAL_HASH) is inserted between the NLJ output and the Agg.
        //   Agg ← LE(LOCAL_HASH) ← NLJ
        //                            ├─ LE(ADAPTIVE_PT) ← LE(PT) ← scan(t1)
        //                            └─ LE(BROADCAST) ← Exchange ← scan(t2)
        setupLocalShuffleSession(null);
        assertPlanShape("select a.k1, count(*) from test.t1 a, test.t2 b where a.k1 > b.k1 group by a.k1",
                anyTree(
                        agg(
                                localExchange(LOCAL_HASH,
                                        nestedLoopJoin(
                                                localExchange(ADAPTIVE_PT,
                                                        localExchange(PT,
                                                                anyTree(olapScan()))),
                                                localExchange(BROADCAST_LE,
                                                        anyTree(exchange())))))));
    }

    @Test
    public void testTopNQualifyPartitionSort() throws Exception {
        // Borrowed from Trino's testTopNRowNumberIsExactlyPartitioned.
        // ROW_NUMBER() filtered by `rn = 1` triggers Doris's PartitionSortNode
        // optimisation (LOCAL phase pre-trims rows before the global Analytic).
        // The chain becomes:
        //   AnalyticEval ← Sort ← LE(LOCAL_HASH) ← PartitionSort ← LE(PT) ← scan
        // The LE(PT) under PartitionSort is the heavy-op fan-out for the pooling scan;
        // the LE(LOCAL_HASH) above PartitionSort enforces hash partitioning for the
        // global ROW_NUMBER computation.
        setupLocalShuffleSession(null);
        assertPlanShape(
                "select k1, k2 from (select k1, k2, row_number() over (partition by k1 order by k2) rn "
                        + "from test.t1) t where rn = 1",
                anyTree(
                        analytic(
                                sort(
                                        localExchange(LOCAL_HASH,
                                                partitionSort(
                                                        localExchange(PT,
                                                                olapScan("t1"))))))));
    }

    @Test
    public void testAggOverBroadcastJoin() throws Exception {
        // Borrowed from Trino's testGroupedAggregationAboveUnionAll variant.
        // count(*) over a broadcast join generates a two-phase aggregation; the
        // partial agg sits directly on top of the HashJoin and the final agg lives
        // in a separate fragment (count merge):
        //   FINAL Agg ← Exchange ← PARTIAL Agg ← HashJoin
        //                                            ├─ LE(PT) ← scan
        //                                            └─ LE(PASS_TO_ONE) ← Exchange ← scan
        setupLocalShuffleSession(null);
        assertPlanShape("select count(*) from test.t1 a join [broadcast] test.t2 b on a.k1=b.k1",
                anyTree(
                        agg(
                                anyTree(
                                        agg(
                                                hashJoin(
                                                        localExchange(PT,
                                                                olapScan()),
                                                        localExchange(PASS_TO_ONE_LE,
                                                                anyTree(exchange()))))))));
    }

    @Test
    public void testNonSerialScanKeepsBucketHashDistribution() throws Exception {
        // Non-pooling scan with pipelineTaskNum=1 → the BUCKET_HASH_SHUFFLE output of
        // the colocated scan is preserved end-to-end; no LOCAL_EXECUTION_HASH_SHUFFLE
        // is ever introduced.  Only a serial-source PASSTHROUGH appears (for the
        // SortNode's merge-by-exchange).
        setupLocalShuffleSession(sv -> {
            sv.setIgnoreStorageDataDistribution(false);
            try {
                sv.setPipelineTaskNum("1");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertNoLocalExchangeOfType(
                "select k1, count(*) from test.t1 group by k1 order by k1",
                LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE);
    }

    @Test
    public void testJoinPlanContainsHashShuffle() throws Exception {
        // Pooling hash join under an aggregate.  Both sides of the join feed through
        // local exchanges; the agg above the join requires LOCAL_EXECUTION_HASH_SHUFFLE.
        //   Agg → LE(LOCAL_HASH) → HashJoin
        //                            ← LE(PASSTHROUGH) ← OlapScan(t1)  (probe)
        //                            ← LE(PASS_TO_ONE) ← Exchange ← OlapScan(t2)  (build)
        setupLocalShuffleSession(null);
        assertPlanShape(
                "select a.k1, count(*) from test.t1 a join test.t2 b on a.k1 = b.k1 group by a.k1",
                anyTree(
                        agg(
                                localExchange(LOCAL_HASH,
                                        hashJoin(
                                                localExchange(PT,
                                                        olapScan("t1")),
                                                localExchange(PASS_TO_ONE_LE,
                                                        anyTree(exchange())))))));
    }

    @Test
    public void testNoopLocalExchangeNotInjected() throws Exception {
        // A simple LIMIT scan plan should contain no local exchanges of any kind —
        // and most importantly, no synthesized NOOP node.  doc rule "NOOP is meta,
        // never materialized": the planner uses NOOP as a 'skip' signal during
        // resolution but never instantiates a LocalExchangeNode with type NOOP.
        setupLocalShuffleSession(null);
        assertNoLocalExchangeOfType("select * from test.t1 limit 1", LocalExchangeType.NOOP);
    }

    @Test
    public void testHashShuffleHasDistributeExprs() throws Exception {
        // Same scan→agg plan as testAggFromScanShapeDsl, but with a predicate that
        // checks the inserted LE(LOCAL_HASH) carries non-empty distribute expressions
        // (without which the BE would not know which columns to hash on).
        setupLocalShuffleSession(null);
        assertPlanShape("select k1, k2, count(*) from test.t1 group by k1, k2",
                anyTree(
                        agg(
                                localExchange(LOCAL_HASH,
                                        localExchange(PT, olapScan("t1")))
                                        .where(le -> le.getDistributeExprLists() != null
                                                && !le.getDistributeExprLists().isEmpty()))));
    }

    @Test
    public void testStreamingAggHashShuffleUsesGroupingExprs() throws Exception {
        // Regression for: FE-planned LE(LOCAL_HASH) under a streaming partial agg used
        // the child's table distribution (e.g. `k1`) instead of the grouping keys
        // (e.g. `k2`).  BE's AggSinkOperatorX/StreamingAggOperatorX::update_operator
        // picks `_partition_exprs = grouping_exprs` when the chain is NOT followed by
        // a shuffled operator (the common case where the streaming preagg sits at
        // fragment root with only a cross-fragment HASH ExchangeSink above).  Using
        // child distribution instead scatters same-group rows across N instances,
        // turning the partial preagg into a no-op and corrupting row-arrival order at
        // downstream merge-finalize (manifests as e.g. non-deterministic
        // group_concat / py_json_array_agg output).
        //
        // Table t1 is DISTRIBUTED BY HASH(k1).  GROUP BY k2 forces a cross-fragment
        // exchange (and thus a two-phase aggregation): the streaming partial agg lives
        // in the lower fragment, with an FE-inserted LE(LOCAL_HASH) below it.  The fix
        // makes that LE carry [k2] (the grouping key) rather than [k1] (the child
        // table distribution).
        setupLocalShuffleSession(null);
        assertPlanShape(
                "select k2, count(*) from test.t1 group by k2",
                anyTree(
                        agg(
                                localExchange(LOCAL_HASH,
                                        localExchange(PT, olapScan("t1")))
                                        .where(le -> {
                                            List<org.apache.doris.analysis.Expr> exprs =
                                                    le.getDistributeExprLists();
                                            if (exprs == null || exprs.size() != 1) {
                                                return false;
                                            }
                                            org.apache.doris.analysis.Expr e = exprs.get(0);
                                            return e instanceof org.apache.doris.analysis.SlotRef
                                                    && "k2".equals(((org.apache.doris.analysis.SlotRef) e).getCol());
                                        }))));
    }

    @Test
    public void testRequireHashSatisfyAllHashShuffleTypes() {
        LocalExchangeNode.LocalExchangeTypeRequire requireHash = LocalExchangeNode.LocalExchangeTypeRequire.requireHash();
        Assertions.assertTrue(requireHash.satisfy(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE));
        Assertions.assertTrue(requireHash.satisfy(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE));
        Assertions.assertTrue(requireHash.satisfy(LocalExchangeType.BUCKET_HASH_SHUFFLE));
        Assertions.assertFalse(requireHash.satisfy(LocalExchangeType.PASSTHROUGH));
    }

    @Test
    public void testSetOperationUnderAggHasHashShuffle() throws Exception {
        // Non-pooling UNION ALL under an agg.  The outer agg's group key requires a
        // LOCAL_EXECUTION_HASH_SHUFFLE directly above the UnionNode (above each
        // branch's pre-agg).
        setupLocalShuffleSession(sv -> sv.setIgnoreStorageDataDistribution(false));
        assertPlanShape(
                "select k1, count(*) from (select k1 from test.t1 union all select k1 from test.t2) u group by k1",
                anyTree(
                        agg(
                                localExchange(LOCAL_HASH,
                                        union(
                                                anyTree(olapScan()),
                                                anyTree(olapScan()))))));
    }

    @Test
    public void testAnalyticPlanContainsPassthroughAndLocalHashShuffle() throws Exception {
        // doc rule "Analytic / 有 partition by / 池化": LE(LOCAL_HASH) for partition
        // redistribution, plus a LE(PASSTHROUGH) heavy-op fan-out below it for the
        // 1-task pooling scan, then LE(PASSTHROUGH) above the AnalyticEval for the
        // outer order-by merge.
        //   SortNode → LE(PASSTHROUGH) → AnalyticEval → SortNode
        //                                     → LE(LOCAL_HASH) → LE(PASSTHROUGH) → scan
        setupLocalShuffleSession(null);
        assertPlanShape(
                "select k1, k2, row_number() over(partition by k1 order by k2) from test.t1 order by k1, k2",
                anyTree(
                        sort(
                                localExchange(PT,
                                        analytic(
                                                sort(
                                                        localExchange(LOCAL_HASH,
                                                                localExchange(PT,
                                                                        olapScan("t1")))))))));
    }

    @Test
    public void testGroupingSetsPlanContainsHashShuffle() throws Exception {
        // Non-pooling grouping sets keeps the colocated BUCKET_HASH_SHUFFLE output of
        // the scan all the way through Repeat→Agg; no LE(LOCAL_HASH) is needed.
        setupLocalShuffleSession(sv -> sv.setIgnoreStorageDataDistribution(false));
        assertNoLocalExchangeOfType(
                "select k1, k2, sum(v1) from test.t1 group by grouping sets((k1), (k1, k2))",
                LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE);
    }

    @Test
    public void testRepeatNoRequireKeepsHashLocalExchangeAboveRepeat() throws Exception {
        // Behavior 1 of the RepeatNode fix — noRequire (tpcds q67, +73%).
        // RepeatNode recurses with noRequire() instead of forwarding the streaming
        // agg's HASH require to its child.  So when the pooling scan upstream does NOT
        // already provide the distribution, the parent inserts the LE(LOCAL_HASH)
        // ABOVE the Repeat, never below it:
        //   Agg <- LE(LOCAL_HASH) <- LE(PASSTHROUGH) <- Repeat <- scan
        // Pinning Repeat with repeat() (not anyTree) distinguishes the fixed plan from
        // the buggy one (buggy forwarded the require, so the LE landed below the
        // Repeat, hashing the pre-repeat rows by the child's single upstream key and
        // collapsing them onto one instance).
        setupLocalShuffleSession(null);
        assertPlanShape(
                "select k1, k2, count(*) from test.t1 group by grouping sets((k1), (k1, k2))",
                anyTree(
                        agg(
                                localExchange(LOCAL_HASH,
                                        localExchange(PT,
                                                repeat(anyTree(olapScan("t1"))))))));
    }

    @Test
    public void testRepeatReturnsChildDistributionSkipsRedundantHash() throws Exception {
        // Behavior 2 of the RepeatNode fix — return enforceResult.second (tpcds q70).
        // RepeatNode reports its child's real output distribution to the parent (not
        // NOOP).  With a non-pooling colocate scan, the child's BUCKET_HASH
        // distribution propagates through the Repeat and already satisfies the agg's
        // hash requirement, so the parent's satisfy-check SKIPS inserting any LE — no
        // LOCAL_HASH appears.  Had RepeatNode returned NOOP (the discarded v1), the
        // satisfy-check would fail and force a redundant LE(LOCAL_HASH) that
        // re-shuffles the post-repeat rows into skew.
        setupLocalShuffleSession(sv -> sv.setIgnoreStorageDataDistribution(false));
        assertNoLocalExchangeOfType(
                "select k1, k2, count(*) from test.t1 group by grouping sets((k1), (k1, k2))",
                LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE);
    }

    @Test
    public void testLocalAndGlobalExecutionHashShufflePreferType() {
        LocalExchangeNode.LocalExchangeTypeRequire requireHash = LocalExchangeNode.LocalExchangeTypeRequire.requireHash();
        LocalExchangeNode.LocalExchangeTypeRequire requireBucketHash
                = LocalExchangeNode.LocalExchangeTypeRequire.requireBucketHash();
        LocalExchangeNode.LocalExchangeTypeRequire requireGlobalHash
                = LocalExchangeNode.LocalExchangeTypeRequire.requireGlobalExecutionHash();

        LocalExchangeType localType = AddLocalExchange.resolveExchangeType(requireHash);
        LocalExchangeType globalType = AddLocalExchange.resolveExchangeType(requireHash);
        // Explicit GLOBAL_EXECUTION_HASH_SHUFFLE must NOT be degraded, even on a scan path.
        // If it appears on a scan path, the plan is wrong — not something resolveExchangeType should fix.
        LocalExchangeType explicitGlobalOnScanType = AddLocalExchange.resolveExchangeType(requireGlobalHash);

        // shouldUseLocalExecutionHash always returns true → RequireHash always resolves to LOCAL
        Assertions.assertEquals(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE, localType);
        Assertions.assertEquals(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE, globalType);
        // Explicit GLOBAL (RequireSpecific) must NOT be degraded.
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, explicitGlobalOnScanType);
        Assertions.assertEquals(LocalExchangeType.BUCKET_HASH_SHUFFLE, requireBucketHash.preferType());
    }

    @Test
    public void testMixedPlanWithPoolingScan() throws Exception {
        // Pooling: grouping-sets sub-query JOINed and re-aggregated.  Probe side
        // wraps the inner agg/Repeat with LE(LOCAL_HASH) over LE(PASSTHROUGH); build
        // side comes through LE(PASS_TO_ONE) over an inter-fragment Exchange.
        //   Agg → HashJoin
        //           ← Agg → LE(LOCAL_HASH) → LE(PASSTHROUGH) → Repeat → scan(t1)
        //           ← LE(PASS_TO_ONE) → Exchange → scan(t2)
        setupLocalShuffleSession(null);
        assertPlanShape(
                "select u.k1, count(*) from (select k1, k2 from test.t1 group by grouping sets((k1), (k1, k2))) u "
                        + "join test.t2 b on u.k1 = b.k1 group by u.k1",
                anyTree(
                        agg(
                                hashJoin(
                                        agg(
                                                localExchange(LOCAL_HASH,
                                                        localExchange(PT,
                                                                anyTree(olapScan("t1"))))),
                                        localExchange(PASS_TO_ONE_LE,
                                                anyTree(exchange()))))));
    }

    @Test
    public void testUnionAllScanAndValues() throws Exception {
        // Tier B (borrowed from Trino): UNION ALL of a real OlapScan and inline
        // VALUES rows.  The values branches flow through their own fragments and
        // land at a UnionNode that gathers via Exchange.  Verifies the
        // scan-side LE(PASSTHROUGH) heavy-op fan-out is still inserted while the
        // values branches contribute no local exchanges (already serial sources).
        setupLocalShuffleSession(null);
        assertPlanShape("select k1 from test.t1 union all select 1 union all select 2",
                anyTree(
                        union(
                                anyTree(exchange()))));
    }

    private EnumSet<LocalExchangeType> collectLocalExchangeTypes(List<PlanFragment> fragments) {
        EnumSet<LocalExchangeType> types = EnumSet.noneOf(LocalExchangeType.class);
        for (PlanFragment fragment : fragments) {
            collect(fragment.getPlanRoot(), types);
        }
        return types;
    }

    private List<LocalExchangeNode> collectLocalExchangeNodes(List<PlanFragment> fragments) {
        List<LocalExchangeNode> nodes = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            collectLocalExchangeNode(fragment.getPlanRoot(), nodes);
        }
        return nodes;
    }

    private String collectFragmentExplain(List<PlanFragment> fragments) {
        StringBuilder explain = new StringBuilder();
        for (PlanFragment fragment : fragments) {
            explain.append(fragment.getExplainString(TExplainLevel.NORMAL));
        }
        return explain.toString();
    }

    private void collect(PlanNode node, EnumSet<LocalExchangeType> types) {
        if (node instanceof LocalExchangeNode) {
            types.add(((LocalExchangeNode) node).getExchangeType());
        }
        for (PlanNode child : node.getChildren()) {
            collect(child, types);
        }
    }

    private void collectLocalExchangeNode(PlanNode node, List<LocalExchangeNode> nodes) {
        if (node instanceof LocalExchangeNode) {
            nodes.add((LocalExchangeNode) node);
        }
        for (PlanNode child : node.getChildren()) {
            collectLocalExchangeNode(child, nodes);
        }
    }

    private static class MockPlanNode extends PlanNode {
        MockPlanNode(PlanNodeId id) {
            super(id, "MOCK-PLAN");
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }

        @Override
        public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
            return "";
        }
    }

    private static class MockScanNode extends ScanNode {
        MockScanNode(PlanNodeId id) {
            super(id, new TupleDescriptor(new TupleId(id.asInt())), "MOCK-SCAN", ScanContext.EMPTY);
        }

        @Override
        protected void createScanRangeLocations() throws UserException {
        }

        @Override
        public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
            return java.util.Collections.emptyList();
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }

        @Override
        public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
            return "";
        }
    }
}
