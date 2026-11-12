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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.MaterializationNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RepeatNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.thrift.TSortAlgorithm;
import org.apache.doris.thrift.TTopnFilterDesc;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * topn filter
 */
public class TopnFilter {
    public TopN topn;
    public SortNode legacySortNode;
    public Map<PhysicalRelation, Expression> targets = Maps.newHashMap();
    public Map<ScanNode, Expr> legacyTargets = Maps.newHashMap();

    private boolean strictEligible;

    public TopnFilter(TopN topn, PhysicalRelation rel, Expression expr, boolean strictEligible) {
        this.topn = topn;
        this.strictEligible = strictEligible;
        targets.put(rel, expr);
    }

    public void addTarget(PhysicalRelation rel, Expression expr, boolean strictEligible) {
        // AND every registered path, including repeated registrations of the same scan.
        this.strictEligible &= strictEligible;
        targets.put(rel, expr);
    }

    /** Eligibility accumulated during pushdown; translated execution metadata is checked separately. */
    public boolean isStrictEligible() {
        return strictEligible;
    }

    public boolean hasTargetRelation(PhysicalRelation rel) {
        return targets.containsKey(rel);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(topn).append(" strictEligible=").append(strictEligible).append("->[ ");
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
        // Reuse the branch's per-target wire format, but upgrade a source only as a whole.
        tFilter.setStrictTargetNodeIds(allowsStrictBounds()
                ? legacyTargets.keySet().stream().map(scan -> scan.getId().asInt())
                        .collect(Collectors.toList()) : new ArrayList<>());
        return tFilter;
    }

    /** Check the final execution plan, since scheduling can change the sort algorithm and limits. */
    public boolean allowsStrictBounds() {
        if (!strictEligible || legacyTargets.isEmpty()
                || legacySortNode.getSortAlgorithm() != TSortAlgorithm.HEAP_SORT
                || legacySortNode.getSortInfo().getOrderingExprs().size() != 1
                || legacySortNode.getLimit() <= 0 || legacySortNode.getOffset() < 0
                || Utils.addOverflows(legacySortNode.getLimit(), legacySortNode.getOffset())) {
            return false;
        }
        Map<ScanNode, Boolean> strictTargets = Maps.newHashMap();
        for (PlanNode child : legacySortNode.getChildren()) {
            collectStrictTargets(child, true, strictTargets);
        }
        return strictTargets.size() == legacyTargets.size()
                && strictTargets.values().stream().allMatch(allowed -> allowed);
    }

    private void collectStrictTargets(PlanNode node, boolean pathStrict, Map<ScanNode, Boolean> strictTargets) {
        // Removing a tie below an intermediate LIMIT/OFFSET can let a different row pass it.
        pathStrict &= !node.hasLimit() && node.getOffset() == 0 && allowsStrictNode(node);
        if (legacyTargets.containsKey(node)) {
            strictTargets.merge((ScanNode) node, pathStrict, (previous, current) -> previous && current);
        }
        // FE retains the sending fragment below ExchangeNode, unlike the serialized BE tree.
        for (PlanNode child : node.getChildren()) {
            collectStrictTargets(child, pathStrict, strictTargets);
        }
    }

    // Nereids proves expression bindings and complete path semantics before registration.
    // Recheck translated readers and limits, which can change after that proof.
    private boolean allowsStrictNode(PlanNode node) {
        if (node instanceof OlapScanNode) {
            OlapScanNode scan = (OlapScanNode) node;
            KeysType keysType = scan.getSelectedIndexKeyType();
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
                    return true;
                default:
                    return false;
            }
        }
        // Repeat is admitted only after the common grouping-key check in Nereids.
        return node instanceof SelectNode || node instanceof SortNode || node instanceof RepeatNode
                || node instanceof ExchangeNode || node instanceof LocalExchangeNode
                || node instanceof SetOperationNode || node instanceof MaterializationNode;
    }
}
