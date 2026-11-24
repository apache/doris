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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.datasource.scan.FileScanNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.MaterializationNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.planner.TableFunctionNode;
import org.apache.doris.thrift.TSortAlgorithm;
import org.apache.doris.thrift.TTopnFilterDesc;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * topn filter
 */
public class TopnFilter {
    public TopN topn;
    public SortNode legacySortNode;
    public Map<PhysicalRelation, Expression> targets = Maps.newHashMap();
    public Map<ScanNode, Expr> legacyTargets = Maps.newHashMap();

    public TopnFilter(TopN topn, PhysicalRelation rel, Expression expr) {
        this.topn = topn;
        targets.put(rel, expr);
    }

    public void addTarget(PhysicalRelation rel, Expression expr) {
        targets.put(rel, expr);
    }

    public boolean hasTargetRelation(PhysicalRelation rel) {
        return targets.containsKey(rel);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(topn).append("->[ ");
        for (PhysicalRelation rel : targets.keySet()) {
            builder.append("(").append(rel).append(":").append(targets.get(rel)).append(") ");
        }
        builder.append("]");
        return builder.toString();
    }

    /**
     * to thrift
     */
    public TTopnFilterDesc toThrift() {
        TTopnFilterDesc tFilter = new TTopnFilterDesc();
        tFilter.setSourceNodeId(legacySortNode.getId().asInt());
        tFilter.setIsAsc(topn.getOrderKeys().get(0).isAsc());
        tFilter.setNullFirst(topn.getOrderKeys().get(0).isNullFirst());
        for (ScanNode scan : legacyTargets.keySet()) {
            tFilter.putToTargetNodeIdToTargetExpr(scan.getId().asInt(),
                    ExprToThriftVisitor.treeToThrift(legacyTargets.get(scan)));
        }
        List<Integer> strictTargets = new ArrayList<>();
        // Scheduling can change the sort algorithm after translation. Inspect the final
        // execution plan here, after ThriftPlansBuilder.setRuntimePredicateIfNeed().
        // Multiple keys must keep ties: (1, 2) can still beat (1, 9) with the same leading key.
        if (legacySortNode.getSortAlgorithm() == TSortAlgorithm.HEAP_SORT
                && legacySortNode.getSortInfo().getOrderingExprs().size() == 1) {
            for (PlanNode child : legacySortNode.getChildren()) {
                collectStrictTargets(child, strictTargets);
            }
        }
        tFilter.setStrictTargetNodeIds(strictTargets);
        return tFilter;
    }

    private void collectStrictTargets(PlanNode node, List<Integer> strictTargets) {
        if (node.hasLimit() || !allowsStrictBounds(node)) {
            // Removing a tie below an intermediate LIMIT can let a different row pass it.
            return;
        }
        if (legacyTargets.containsKey(node)) {
            strictTargets.add(node.getId().asInt());
        }
        // FE retains the sending fragment below ExchangeNode, unlike the serialized BE tree.
        for (PlanNode child : node.getChildren()) {
            collectStrictTargets(child, strictTargets);
        }
    }

    /**
     * Whether this node preserves the safety of discarding boundary ties at a descendant scan.
     *
     * <p>This refines an already accepted TopN filter; it does not authorize new predicate pushdown.
     * The caller checks the final single-key HEAP_SORT algorithm and rejects intermediate LIMITs.
     * Once the source heap holds LIMIT + OFFSET candidates, another equal key cannot improve that
     * ordering. However, dropping an input tie must not change a surviving output row, create a new
     * output row, suppress an error, or remove input needed to produce a better-ranked row later.
     * This property must hold at every node between the source and the target.
     *
     * <p>The examples below explain local operator semantics, not necessarily plans that current
     * pushdown rules can produce: some shapes are already rejected upstream. Unless stated otherwise,
     * use ascending order with boundary b = 1, so the additional rows rejected by {@code k < b}
     * instead of {@code k <= b} have k = 1. A bound may arrive after some input has been consumed,
     * for example from a safe sibling of a UNION. Thus a merge can lose only part of a boundary group.
     * Descending order has the same issue with the comparison reversed.
     *
     * <p>This is a whitelist: false means that the path retains its inclusive filter, not that every
     * instance of the operator is unsafe. New operator/model variants need their own proof before
     * being admitted, even if a particular example would happen to produce the same result.
     */
    private boolean allowsStrictBounds(PlanNode node) {
        if (node instanceof OlapScanNode) {
            OlapScanNode scan = (OlapScanNode) node;
            KeysType keysType = scan.getSelectedIndexKeyType();
            // Use the selected index's model: a DUP_KEYS base table may have an AGG_KEYS rollup.
            // Rejected storage cases, even when the SQL plan contains no AggregationNode:
            // - Changelog: +(k=1,v=10), -(k=1,v=10) should cancel when folded. If the insert was
            //   consumed before the bound and the delete is filtered afterwards, a spurious insert
            //   survives. All changelog scan modes retain inclusive bounds; this rule does not try
            //   to prove individual modes such as DETAIL independent of storage folding.
            // - AGG_KEYS: stored SUM contributions (k=1,v=10), (k=1,v=20) must produce (1,30).
            //   Reading 10 before the bound and dropping 20 afterwards would instead produce (1,10).
            // - UNIQUE_KEYS without MOW (MOR): old (k=1,v=10), new (k=1,v=20) must resolve to (1,20).
            //   Dropping the new version after the old one was read can expose the obsolete (1,10).
            // - PRIMARY_KEYS/UNKNOWN, or a future model: even a scan containing just (k=1,v=10)
            //   remains inclusive. These labels do not establish the DUP_KEYS/MOW guarantee of
            //   finalized, independently discardable rows in this whitelist; this is conservative.
            // Ordinary DUP_KEYS rows and resolved MOW rows need no such read-time merge of versions
            // or contributions, so their boundary ties can be discarded when the rest of the path allows it.
            return !scan.isReadChangelog() && (keysType == KeysType.DUP_KEYS
                    || (keysType == KeysType.UNIQUE_KEYS && scan.getOlapTable().getEnableUniqueKeyMergeOnWrite()));
        }
        if (node instanceof JoinNodeBase) {
            switch (((JoinNodeBase) node).getJoinOp()) {
                case INNER_JOIN:
                case CROSS_JOIN:
                case LEFT_OUTER_JOIN:
                case LEFT_SEMI_JOIN:
                case LEFT_ANTI_JOIN:
                case NULL_AWARE_LEFT_ANTI_JOIN:
                case NULL_AWARE_LEFT_SEMI_JOIN:
                case ASOF_LEFT_INNER_JOIN:
                case ASOF_LEFT_OUTER_JOIN:
                    return true;
                default:
                    // The registered scan filter is pushed into the left/probe side. For equality
                    // joins, take L={(k=1,v=10)}, R={(k=1)} and remove the sole matching L row:
                    // - RIGHT_OUTER_JOIN: the result changes from (L.v=10,R.k=1) to (NULL,1).
                    // - FULL_OUTER_JOIN: the same example creates the previously absent (NULL,1).
                    // - RIGHT_ANTI_JOIN: the empty result becomes R.k=1, since R is now unmatched.
                    // - RIGHT_SEMI_JOIN: plain semi output {R.k=1} becomes empty; with a mark it
                    //   changes (R.k=1,mark=true) to (1,false). The mark case changes a surviving row.
                    //   Plain right-semi is also excluded conservatively; no separate proof for its
                    //   build-side output is made here. This does not claim all plain semi cases fail.
                    // ASOF examples use the nearest L.t <= R.t, with R.t=2:
                    // - ASOF_RIGHT_INNER_JOIN: L.t={1,2} originally selects 2. Dropping L.t=2
                    //   (boundary b=2 on L.t) selects 1 instead, creating a different joined row.
                    // - ASOF_RIGHT_OUTER_JOIN: with only L.t=2, dropping that boundary row changes
                    //   (L.t=2,R.t=2) to (NULL,2). With L.t={1,2}, it can also switch matches as above.
                    // Future join types stay excluded until their probe-removal semantics are proved.
                    return false;
            }
        }
        // Rejected operator families (all fall through the whitelist below):
        // - AggregationNode/BucketedAggregationNode: GROUP BY k with SUM(v) over (1,10), (1,20)
        //   must return (1,30), not (1,10) if the second contribution is dropped by a late bound.
        //   A scalar COUNT(*) is also sensitive: removing its two inputs changes the result 2 to 0.
        // - RepeatNode: for grouping sets ((k),()), input k=1 expands to k=1 and k=NULL. Dropping
        //   that input also removes the NULL copy, which could rank before the boundary with NULLS FIRST.
        // - AnalyticEvalNode: SUM(v) OVER () on (k,v)={(1,10),(0,20)} gives the k=0 row a sum of 30.
        //   Dropping k=1 changes that surviving, better-ranked row's sum to 20.
        // - AssertNumRowsNode: an at-most-one-row assertion must fail on input k={1,1}. Filtering
        //   both rows hides the error; scalar-subquery handling may also synthesize a new NULL row.
        // - PartitionSortNode: in one partition, ORDER BY t with partition limit 1 chooses (k=1,t=0)
        //   over (k=0,t=1). Filtering k=1 first makes the previously unselected k=0 row survive.
        // - RecursiveCteNode/RecursiveCteScanNode: seed k=1 followed by k-1 while k>0 produces {1,0}.
        //   Dropping the boundary seed loses the later k=0 row, which would beat the bound.
        // - CTEScanNode/shared CTE paths need consumer-specific proof. For CTE input {k=1}, pushing
        //   one consumer's strict bound into the shared producer could change another's COUNT(*) from 1 to 0.
        // - Other unlisted nodes, including unsupported scans and future operators, have no proof here.
        //   For example, TopN -> SchemaScanNode is not an accepted target path; EmptySetNode has no rows
        //   or useful scan target to mark. Neither needs a strict mode. A new operator could instead
        //   behave like the window example above, so it must not be accepted merely for having children.
        // The admitted nodes preserve the established pushdown semantics without requiring additional
        // boundary contributions. Set-operation targets and join probe sides were selected upstream.
        return node instanceof FileScanNode || node instanceof SelectNode || node instanceof SortNode
                || node instanceof ExchangeNode || node instanceof LocalExchangeNode
                || node instanceof SetOperationNode || node instanceof TableFunctionNode
                || node instanceof MaterializationNode;
    }
}
