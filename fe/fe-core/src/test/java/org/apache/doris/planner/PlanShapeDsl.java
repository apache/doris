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

package org.apache.doris.planner;

import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;

import java.util.List;

/**
 * Implement this interface in a test class to use the {@link PlanShape} DSL
 * factories without a {@code PlanShape.} prefix and without {@code static}
 * imports (which are forbidden by Doris's checkstyle rule
 * {@code AvoidStaticImport}).
 *
 * <p>Each factory is a {@code default} method that delegates to the static one
 * on {@link PlanShape}.  The shortened {@code LocalExchangeType} aliases
 * (LOCAL_HASH, GLOBAL_HASH, BUCKET_HASH, etc.) are interface constants so
 * implementing classes can reference them by bare name.
 *
 * <p>Example:
 * <pre>{@code
 * public class MyTest extends TestWithFeService implements PlanShapeDsl {
 *     @Test
 *     public void testAggOverScan() {
 *         assertPlanShape(sql,
 *             anyTree(agg(localExchange(LOCAL_HASH, olapScan("t1")))));
 *     }
 * }
 * }</pre>
 */
public interface PlanShapeDsl {

    // ---- shortened LocalExchangeType aliases ----
    // Avoids spelling LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE in every test.

    LocalExchangeType LOCAL_HASH = LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE;
    LocalExchangeType GLOBAL_HASH = LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE;
    LocalExchangeType BUCKET_HASH = LocalExchangeType.BUCKET_HASH_SHUFFLE;
    LocalExchangeType PT = LocalExchangeType.PASSTHROUGH;
    LocalExchangeType ADAPTIVE_PT = LocalExchangeType.ADAPTIVE_PASSTHROUGH;
    LocalExchangeType BROADCAST_LE = LocalExchangeType.BROADCAST;
    LocalExchangeType PASS_TO_ONE_LE = LocalExchangeType.PASS_TO_ONE;
    LocalExchangeType NOOP_LE = LocalExchangeType.NOOP;

    // ---- structural factories ----

    default <T extends PlanNode> PlanShape<T> node(Class<T> cls, PlanShape<?>... children) {
        return PlanShape.node(cls, children);
    }

    default PlanShape<PlanNode> anyTree(PlanShape<?> inner) {
        return PlanShape.anyTree(inner);
    }

    default PlanShape<PlanNode> anyNode(PlanShape<?>... children) {
        return PlanShape.anyNode(children);
    }

    default PlanShape<PlanNode> anyNot(Class<? extends PlanNode> excludedClass,
                                        PlanShape<?>... children) {
        return PlanShape.anyNot(excludedClass, children);
    }

    // ---- Doris-specific node factories ----

    default PlanShape<LocalExchangeNode> localExchange(LocalExchangeType type,
                                                       PlanShape<?>... children) {
        return PlanShape.localExchange(type, children);
    }

    default PlanShape<AggregationNode> agg(PlanShape<?>... children) {
        return PlanShape.agg(children);
    }

    default PlanShape<OlapScanNode> olapScan(String tableName, PlanShape<?>... children) {
        return PlanShape.olapScan(tableName, children);
    }

    default PlanShape<OlapScanNode> olapScan() {
        return PlanShape.olapScan();
    }

    default PlanShape<ExchangeNode> exchange(PlanShape<?>... children) {
        return PlanShape.exchange(children);
    }

    default PlanShape<HashJoinNode> hashJoin(PlanShape<?>... children) {
        return PlanShape.hashJoin(children);
    }

    default PlanShape<SortNode> sort(PlanShape<?>... children) {
        return PlanShape.sort(children);
    }

    default PlanShape<AnalyticEvalNode> analytic(PlanShape<?>... children) {
        return PlanShape.analytic(children);
    }

    default PlanShape<UnionNode> union(PlanShape<?>... children) {
        return PlanShape.union(children);
    }

    default PlanShape<NestedLoopJoinNode> nestedLoopJoin(PlanShape<?>... children) {
        return PlanShape.nestedLoopJoin(children);
    }

    default PlanShape<PartitionSortNode> partitionSort(PlanShape<?>... children) {
        return PlanShape.partitionSort(children);
    }

    default PlanShape<RepeatNode> repeat(PlanShape<?>... children) {
        return PlanShape.node(RepeatNode.class, children);
    }

    // ---- assertion entry points ----

    default void assertMatches(PlanNode root, PlanShape<?> shape) {
        PlanShape.assertMatches(root, shape);
    }

    default void assertMatchesAnyFragment(List<PlanFragment> fragments, PlanShape<?> shape) {
        PlanShape.assertMatchesAnyFragment(fragments, shape);
    }

    /**
     * Print all fragments' plan trees to stderr.  Useful for one-off debugging
     * when writing a new shape assertion: print first, copy the structure into a
     * {@link PlanShape} pattern, then remove the call.  Not intended to be left
     * in committed tests.
     */
    default void printFragmentPlans(List<PlanFragment> fragments) {
        for (PlanFragment f : fragments) {
            System.err.println("=== fragment " + f.getFragmentId() + " ===");
            System.err.println(PlanShape.prettyPrint(f.getPlanRoot()));
        }
    }
}
