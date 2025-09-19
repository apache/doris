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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.FilterEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a conflict rule maker to
 */
public class ConflictRulesMaker {
    private static ValEntry[][] assocTable = {
        //             inner-B       semi-B        anti-B        left-B         full-B
        /* inner-A */ {ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.NO},
        /* semi-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO},
        /* anti-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO},
        /* left-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.BRejectRA, ValEntry.NO},
        /* full-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.BRejectRA, ValEntry.ABRejectRA},
    };

    private static ValEntry[][] leftAsscomTable = {
        //             inner-B       semi-B        anti-B        left-B         full-B
        /* inner-A */ {ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.NO},
        /* semi-A  */ {ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.NO},
        /* anti-A  */ {ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.NO},
        /* left-A  */ {ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.YES, ValEntry.ARejectLA},
        /* full-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.BRejectRB, ValEntry.ABRejectLA},
    };

    private static ValEntry[][] rightAsscomTable = {
        //             inner-B       semi-B        anti-B        left-B         full-B
        /* inner-A */ {ValEntry.YES, ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO},
        /* semi-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO},
        /* anti-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO},
        /* left-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO},
        /* full-A  */ {ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.NO, ValEntry.ABRejectRB},
    };

    private ConflictRulesMaker() {}

    /**
     * make conflict rules for filter edges
     */
    public static void makeFilterConflictRules(
            JoinEdge joinEdge, List<JoinEdge> joinEdges, List<FilterEdge> filterEdges) {
        long leftSubNodes = joinEdge.getLeftSubNodes(joinEdges);
        long rightSubNodes = joinEdge.getRightSubNodes(joinEdges);
        filterEdges.forEach(e -> {
            if (LongBitmap.isSubset(e.getReferenceNodes(), leftSubNodes)
                    && !PushDownFilterThroughJoin.COULD_PUSH_THROUGH_LEFT.contains(joinEdge.getJoinType())) {
                e.addLeftRejectEdge(joinEdge);
            }
            if (LongBitmap.isSubset(e.getReferenceNodes(), rightSubNodes)
                    && !PushDownFilterThroughJoin.COULD_PUSH_THROUGH_RIGHT.contains(joinEdge.getJoinType())) {
                e.addRightRejectEdge(joinEdge);
            }
        });
    }

    /**
     * Make edge with CD-C algorithm in
     * On the correct and complete enumeration of the core search
     */
    public static void makeJoinConflictRules(JoinEdge edgeB, List<JoinEdge> joinEdges) {
        // find all left and right subtree edges and ready for CD-C check
        BitSet leftSubTreeEdges = subTreeEdges(edgeB.getLeftChildEdges(), joinEdges);
        BitSet rightSubTreeEdges = subTreeEdges(edgeB.getRightChildEdges(), joinEdges);
        long leftRequired = edgeB.getLeftRequiredNodes();
        long rightRequired = edgeB.getRightRequiredNodes();

        for (int i = leftSubTreeEdges.nextSetBit(0); i >= 0; i = leftSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!JoinType.isAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getLeftSubNodes(joinEdges));
                childA.addLeftRejectEdge(edgeB);
            }
            if (!JoinType.isLAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getRightSubNodes(joinEdges));
                childA.addLeftRejectEdge(edgeB);
            }
        }

        for (int i = rightSubTreeEdges.nextSetBit(0); i >= 0; i = rightSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!JoinType.isAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getRightSubNodes(joinEdges));
                childA.addRightRejectEdge(edgeB);
            }
            if (!JoinType.isRAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getLeftSubNodes(joinEdges));
                childA.addRightRejectEdge(edgeB);
            }
        }
        edgeB.setLeftExtendedNodes(leftRequired);
        edgeB.setRightExtendedNodes(rightRequired);
    }

    public static void makeJoinConflictRules(JoinEdge edgeB, List<JoinEdge> joinEdges, ExpressionRewriteContext ctx) {
        // find all left and right subtree edges and ready for CD-C check
        BitSet leftSubTreeEdges = subTreeEdges(edgeB.getLeftChildEdges(), joinEdges);
        BitSet rightSubTreeEdges = subTreeEdges(edgeB.getRightChildEdges(), joinEdges);
        List<Pair<Long, Long>> conflictRules = new ArrayList<>();
        for (int i = leftSubTreeEdges.nextSetBit(0); i >= 0; i = leftSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!isAssocLeftTree(childA, edgeB, ctx)) {
                generateAssocLeftTreeCR(childA, conflictRules);
                // for HyperGraphComparator
                childA.addLeftRejectEdge(edgeB);
            }
            if (!isLAssoc(childA, edgeB, ctx)) {
                generateLAssocCR(childA, conflictRules);
                // for HyperGraphComparator
                childA.addLeftRejectEdge(edgeB);
            }
        }

        for (int i = rightSubTreeEdges.nextSetBit(0); i >= 0; i = rightSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!isAssocRightTree(edgeB, childA, ctx)) {
                generateAssocRightTreeCR(childA, conflictRules);
                // for HyperGraphComparator
                childA.addRightRejectEdge(edgeB);
            }
            if (!isRAssoc(edgeB, childA, ctx)) {
                generateRAssocCR(childA, conflictRules);
                // for HyperGraphComparator
                childA.addRightRejectEdge(edgeB);
            }
        }

        long tes = simplifyConflictRules(edgeB.getRequireNodes(), conflictRules);
        if (!LongBitmap.isOverlap(tes, edgeB.getLeftSubtreeNodes())) {
            tes = LongBitmap.or(tes, edgeB.getLeftSubtreeNodes());
            tes = simplifyConflictRules(tes, conflictRules);
        }
        if (!LongBitmap.isOverlap(tes, edgeB.getRightSubtreeNodes())) {
            tes = LongBitmap.or(tes, edgeB.getRightSubtreeNodes());
            tes = simplifyConflictRules(tes, conflictRules);
        }
        edgeB.setLeftExtendedNodes(LongBitmap.and(tes, edgeB.getLeftSubtreeNodes()));
        edgeB.setRightExtendedNodes(LongBitmap.and(tes, edgeB.getRightSubtreeNodes()));
        edgeB.setConflictRules(conflictRules);
    }

    private static long simplifyConflictRules(long tes, List<Pair<Long, Long>> conflictRules) {
        long oldTes;
        do {
            oldTes = tes;
            for (Pair<Long, Long> rule : conflictRules) {
                if (LongBitmap.isOverlap(rule.first, tes)) {
                    tes = LongBitmap.or(tes, rule.second);
                }
            }
            final long tempTes = tes;
            conflictRules.removeIf(rule -> LongBitmap.isSubset(rule.second, tempTes));
        } while (tes != oldTes && !conflictRules.isEmpty());
        return tes;
    }

    private static boolean isAssocLeftTree(JoinEdge leftChildEdge, JoinEdge currentEdge, ExpressionRewriteContext ctx) {
        int indexA = getIndexForJoinType(leftChildEdge.getJoinType());
        int indexB = getIndexForJoinType(currentEdge.getJoinType());
        if (indexA >= 0 && indexB >= 0) {
            return isValidToReorder(assocTable[indexA][indexB], leftChildEdge.getJoin(), currentEdge.getJoin(), ctx);
        } else {
            return false;
        }
    }

    private static boolean isAssocRightTree(JoinEdge currentEdge, JoinEdge rightChildEdge, ExpressionRewriteContext ctx) {
        int indexA = getIndexForJoinType(currentEdge.getJoinType());
        int indexB = getIndexForJoinType(rightChildEdge.getJoinType());
        if (indexA >= 0 && indexB >= 0) {
            return isValidToReorder(assocTable[indexA][indexB], currentEdge.getJoin(), rightChildEdge.getJoin(), ctx);
        } else {
            return false;
        }
    }

    private static boolean isLAssoc(JoinEdge leftChildEdge, JoinEdge currentEdge, ExpressionRewriteContext ctx) {
        int indexA = getIndexForJoinType(leftChildEdge.getJoinType());
        int indexB = getIndexForJoinType(currentEdge.getJoinType());
        if (indexA >= 0 && indexB >= 0) {
            return isValidToReorder(leftAsscomTable[indexA][indexB], leftChildEdge.getJoin(), currentEdge.getJoin(), ctx);
        } else {
            return false;
        }
    }

    private static boolean isRAssoc(JoinEdge currentEdge, JoinEdge rightChildEdge, ExpressionRewriteContext ctx) {
        int indexA = getIndexForJoinType(currentEdge.getJoinType());
        int indexB = getIndexForJoinType(rightChildEdge.getJoinType());
        if (indexA >= 0 && indexB >= 0) {
            return isValidToReorder(rightAsscomTable[indexA][indexB], currentEdge.getJoin(), rightChildEdge.getJoin(), ctx);
        } else {
            return false;
        }
    }

    private static BitSet subTreeEdge(Edge edge, List<JoinEdge> joinEdges) {
        long subTreeNodes = edge.getSubTreeNodes();
        BitSet subEdges = new BitSet();
        joinEdges.stream()
                .filter(e -> LongBitmap.isSubset(e.getReferenceNodes(), subTreeNodes))
                .forEach(e -> subEdges.set(e.getIndex()));
        return subEdges;
    }

    private static BitSet subTreeEdges(BitSet edgeSet, List<JoinEdge> joinEdges) {
        BitSet bitSet = new BitSet();
        edgeSet.stream()
                .mapToObj(i -> subTreeEdge(joinEdges.get(i), joinEdges))
                .forEach(bitSet::or);
        return bitSet;
    }

    private enum ValEntry {
        YES,
        NO,
        BRejectRA,
        ABRejectRA,
        ARejectLA,
        BRejectRB,
        ABRejectLA,
        ABRejectRB
    }

    private static int getIndexForJoinType(JoinType joinType) {
        switch (joinType) {
            case CROSS_JOIN:
            case INNER_JOIN:
                return 0;
            case LEFT_SEMI_JOIN:
                return 1;
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return 2;
            case LEFT_OUTER_JOIN:
                return 3;
            case FULL_OUTER_JOIN:
                return 4;
            default:
                return -1;
        }
    }

    private static boolean isValidToReorder(ValEntry valEntry,
                                            LogicalJoin joinA,
                                            LogicalJoin joinB,
                                            ExpressionRewriteContext ctx) {
        switch (valEntry) {
            case YES:
                return true;
            case BRejectRA: {
                Set<Slot> outputRA = joinA.right().getOutputSet();
                for (Object expression : joinB.getExpressions()) {
                    if (isEvalToNullOrFalse(outputRA, (Expression) expression, ctx)) {
                        return true;
                    }
                }
                return false;
            }
            case ABRejectRA: {
                boolean aRejectRA = false;
                Set<Slot> outputRA = joinA.right().getOutputSet();
                for (Object expression : joinA.getExpressions()) {
                    if (isEvalToNullOrFalse(outputRA, (Expression) expression, ctx)) {
                        aRejectRA = true;
                        break;
                    }
                }
                if (aRejectRA) {
                    for (Object expression : joinB.getExpressions()) {
                        if (isEvalToNullOrFalse(outputRA, (Expression) expression, ctx)) {
                            return true;
                        }
                    }
                }
                return false;
            }
            case ARejectLA: {
                Set<Slot> outputLA = joinA.left().getOutputSet();
                for (Object expression : joinA.getExpressions()) {
                    if (isEvalToNullOrFalse(outputLA, (Expression) expression, ctx)) {
                        return true;
                    }
                }
                return false;
            }
            case BRejectRB: {
                Set<Slot> outputRB = joinB.right().getOutputSet();
                for (Object expression : joinB.getExpressions()) {
                    if (isEvalToNullOrFalse(outputRB, (Expression) expression, ctx)) {
                        return true;
                    }
                }
                return false;
            }
            case ABRejectLA: {
                boolean aRejectLA = false;
                Set<Slot> outputLA = joinA.left().getOutputSet();
                for (Object expression : joinA.getExpressions()) {
                    if (isEvalToNullOrFalse(outputLA, (Expression) expression, ctx)) {
                        aRejectLA = true;
                        break;
                    }
                }
                if (aRejectLA) {
                    for (Object expression : joinB.getExpressions()) {
                        if (isEvalToNullOrFalse(outputLA, (Expression) expression, ctx)) {
                            return true;
                        }
                    }
                }
                return false;
            }
            case ABRejectRB: {
                boolean aRejectRB = false;
                Set<Slot> outputRB = joinB.right().getOutputSet();
                for (Object expression : joinA.getExpressions()) {
                    if (isEvalToNullOrFalse(outputRB, (Expression) expression, ctx)) {
                        aRejectRB = true;
                        break;
                    }
                }
                if (aRejectRB) {
                    for (Object expression : joinB.getExpressions()) {
                        if (isEvalToNullOrFalse(outputRB, (Expression) expression, ctx)) {
                            return true;
                        }
                    }
                }
                return false;
            }
            case NO:
            default:
                return false;
        }
    }

    private static boolean isEvalToNullOrFalse(Set<Slot> slots, Expression expression, ExpressionRewriteContext ctx) {
        Map<Slot, NullLiteral> replaceMap = new HashMap<>();
        for (Slot slot : slots) {
            replaceMap.put(slot, new NullLiteral(slot.getDataType()));
        }
        Expression evalExpr = FoldConstantRule.evaluate(
                ExpressionUtils.replace(expression, replaceMap), ctx
        );
        return evalExpr.isNullLiteral() || BooleanLiteral.FALSE.equals(evalExpr);
    }

    private static void generateAssocLeftTreeCR(JoinEdge leftChildEdge, List<Pair<Long, Long>> conflictRules) {
        long childReferencedNodes = leftChildEdge.getReferenceNodes();
        long childLeftSubtreeNodes = leftChildEdge.getLeftSubtreeNodes();
        long childRightSubtreeNodes = leftChildEdge.getRightSubtreeNodes();
        if (LongBitmap.isOverlap(childReferencedNodes, childLeftSubtreeNodes)) {
            conflictRules.add(Pair.of(childRightSubtreeNodes,
                    LongBitmap.newBitmapIntersect(childReferencedNodes, childLeftSubtreeNodes)));
        } else {
            conflictRules.add(Pair.of(childRightSubtreeNodes, childLeftSubtreeNodes));
        }
    }

    private static void generateAssocRightTreeCR(JoinEdge rightChildEdge, List<Pair<Long, Long>> conflictRules) {
        long childReferencedNodes = rightChildEdge.getReferenceNodes();
        long childLeftSubtreeNodes = rightChildEdge.getLeftSubtreeNodes();
        long childRightSubtreeNodes = rightChildEdge.getRightSubtreeNodes();
        if (LongBitmap.isOverlap(childReferencedNodes, childRightSubtreeNodes)) {
            conflictRules.add(Pair.of(childLeftSubtreeNodes,
                    LongBitmap.newBitmapIntersect(childReferencedNodes, childRightSubtreeNodes)));
        } else {
            conflictRules.add(Pair.of(childLeftSubtreeNodes, childRightSubtreeNodes));
        }
    }

    private static void generateLAssocCR(JoinEdge leftChildEdge, List<Pair<Long, Long>> conflictRules) {
        long childReferencedNodes = leftChildEdge.getReferenceNodes();
        long childLeftSubtreeNodes = leftChildEdge.getLeftSubtreeNodes();
        long childRightSubtreeNodes = leftChildEdge.getRightSubtreeNodes();
        if (LongBitmap.isOverlap(childReferencedNodes, childRightSubtreeNodes)) {
            conflictRules.add(Pair.of(childLeftSubtreeNodes,
                    LongBitmap.newBitmapIntersect(childReferencedNodes, childRightSubtreeNodes)));
        } else {
            conflictRules.add(Pair.of(childLeftSubtreeNodes, childRightSubtreeNodes));
        }
    }

    private static void generateRAssocCR(JoinEdge rightChildEdge, List<Pair<Long, Long>> conflictRules) {
        long childReferencedNodes = rightChildEdge.getReferenceNodes();
        long childLeftSubtreeNodes = rightChildEdge.getLeftSubtreeNodes();
        long childRightSubtreeNodes = rightChildEdge.getRightSubtreeNodes();
        if (LongBitmap.isOverlap(childReferencedNodes, childLeftSubtreeNodes)) {
            conflictRules.add(Pair.of(childRightSubtreeNodes,
                    LongBitmap.newBitmapIntersect(childReferencedNodes, childLeftSubtreeNodes)));
        } else {
            conflictRules.add(Pair.of(childRightSubtreeNodes, childLeftSubtreeNodes));
        }
    }
}
