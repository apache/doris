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

package org.apache.doris.nereids.hint;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

/**
 * select hint.
 * e.g. set_var(query_timeout='1800', exec_mem_limit='2147483648')
 */
public class LeadingHint extends Hint {
    private String originalString = "";

    private List<String> addJoinParameters;
    private List<String> normalizedParameters;
    private final List<String> tablelist = new ArrayList<>();

    private final Map<RelationId, LogicalPlan> relationIdToScanMap = Maps.newLinkedHashMap();

    private final List<Pair<RelationId, String>> relationIdAndTableName = new ArrayList<>();

    private final Map<ExprId, String> exprIdToTableNameMap = Maps.newLinkedHashMap();

    /** A filter occurrence with its table bitmap, expression, and the original join type
     *  from which it was collected. The original type is used to prevent a filter from
     *  being consumed at the wrong join level (e.g., an inner-join predicate with a
     *  narrow bitmap being consumed by a lower anti join). */
    public static class FilterEntry {
        public final long bitmap;
        public final Expression expr;
        public final JoinType originalType;

        public FilterEntry(long bitmap, Expression expr, JoinType originalType) {
            this.bitmap = bitmap;
            this.expr = expr;
            this.originalType = originalType;
        }
    }

    private final List<FilterEntry> filters = new ArrayList<>();

    private final List<JoinConstraint> joinConstraintList = new ArrayList<>();

    private Long innerJoinBitmap = 0L;

    private Long totalBitmap = 0L;

    private final Map<String, DistributeHint> strToHint = new HashMap<>();

    public LeadingHint(String hintName) {
        super(hintName);
    }

    /**
     * Leading hint data structure before using
     * @param hintName Leading
     * @param parameters table name mixed with left and right brace
     */
    public LeadingHint(String hintName, List<String> parameters, String originalString,
            Map<String, DistributeHint> strToHint) {
        super(hintName);
        this.originalString = originalString;
        addJoinParameters = insertJoinIntoParameters(parameters);
        normalizedParameters = parseIntoReversePolishNotation(addJoinParameters);
        this.strToHint.putAll(strToHint);
    }

    /**
     * insert join string into leading string
     * @param list of sql input leading string
     * @return list of string adding joins into tables
     */
    public static List<String> insertJoinIntoParameters(List<String> list) {
        List<String> output = new ArrayList<>();

        for (String item : list) {
            if (item.startsWith("broadcast") || item.startsWith("shuffle")) {
                output.remove(output.size() - 1);
                output.add(item);
                continue;
            } else if (item.equals("{")) {
                output.add(item);
                continue;
            } else if (item.equals("}")) {
                output.remove(output.size() - 1);
                output.add(item);
            } else {
                output.add(item);
            }
            output.add("join");
        }
        output.remove(output.size() - 1);
        return output;
    }

    /**
     * parse list string of original leading string with join string to Reverse Polish notation
     * @param list of leading with join string
     * @return Reverse Polish notation which can be used directly changed into logical join
     */
    public List<String> parseIntoReversePolishNotation(List<String> list) {
        Stack<String> s1 = new Stack<>();
        List<String> s2 = new ArrayList<>();

        for (String item : list) {
            if (!(item.startsWith("shuffle") || item.startsWith("broadcast") || item.equals("{")
                    || item.equals("}") || item.equals("join"))) {
                tablelist.add(item);
                s2.add(item);
            } else if (item.equals("{")) {
                s1.push(item);
            } else if (item.equals("}")) {
                while (!s1.peek().equals("{")) {
                    String pop = s1.pop();
                    s2.add(pop);
                }
                s1.pop();
            } else {
                while (s1.size() != 0 && !s1.peek().equals("{")) {
                    s2.add(s1.pop());
                }
                s1.push(item);
            }
        }
        while (s1.size() > 0) {
            s2.add(s1.pop());
        }
        return s2;
    }

    public List<String> getTablelist() {
        return tablelist;
    }

    public Map<RelationId, LogicalPlan> getRelationIdToScanMap() {
        return relationIdToScanMap;
    }

    @Override
    public String getExplainString() {
        if (!this.isSuccess()) {
            return originalString;
        }
        StringBuilder out = new StringBuilder();
        for (String parameter : addJoinParameters) {
            if (parameter.equals("{") || parameter.equals("}") || parameter.equals("[") || parameter.equals("]")) {
                out.append(parameter + " ");
            } else if (parameter.startsWith("shuffle") || parameter.startsWith("broadcast")) {
                DistributeHint distributeHint = strToHint.get(parameter);
                if (!distributeHint.isSuccess()) {
                    continue;
                }
                if (distributeHint.distributeType.equals(DistributeType.SHUFFLE_RIGHT)) {
                    out.append("shuffle");
                    if (distributeHint.getSkewInfo() != null && distributeHint.isSuccessInSkewRewrite()) {
                        out.append("_skew ");
                    } else {
                        out.append(" ");
                    }
                } else {
                    out.append("broadcast ");
                }
            } else if (parameter.equals("join")) {
                continue;
            } else {
                out.append(parameter + " ");
            }
        }
        return "leading(" + out.toString() + ")";
    }

    /**
     * Get logical plan by table name recorded in leading hint. if can not get, means leading has syntax error
     * or need to update. So return null should be deal with when call
     * @param name table name
     * @return logical plan recorded when binding
     */
    public LogicalPlan getLogicalPlanByName(String name) {
        RelationId id = findRelationIdAndTableName(name);
        if (id == null) {
            this.setStatus(HintStatus.SYNTAX_ERROR);
            this.setErrorMessage("can not find table: " + name);
            return null;
        }
        return relationIdToScanMap.get(id);
    }

    /**
     * putting pair into list, if relation id already exist update table name
     * @param relationIdTableNamePair pair of relation id and table name to be inserted
     */
    public void putRelationIdAndTableName(Pair<RelationId, String> relationIdTableNamePair) {
        boolean isUpdate = false;
        for (Pair<RelationId, String> pair : relationIdAndTableName) {
            if (pair.first.equals(relationIdTableNamePair.first)) {
                pair.second = relationIdTableNamePair.second;
                isUpdate = true;
            }
            if (pair.second.equals(relationIdTableNamePair.second)) {
                pair.first = relationIdTableNamePair.first;
                isUpdate = true;
            }
        }
        if (!isUpdate) {
            relationIdAndTableName.add(relationIdTableNamePair);
        }
    }

    /**
     * putting pair into list, if relation id already exist update table name
     * @param relationIdTableNamePair pair of relation id and table name to be inserted
     */
    public void updateRelationIdByTableName(Pair<RelationId, String> relationIdTableNamePair) {
        boolean isUpdate = false;
        for (Pair<RelationId, String> pair : relationIdAndTableName) {
            if (pair.second.equals(relationIdTableNamePair.second)) {
                pair.first = relationIdTableNamePair.first;
                isUpdate = true;
            }
        }
        if (!isUpdate) {
            relationIdAndTableName.add(relationIdTableNamePair);
        }
    }

    /**
     * find relation id and table name pair, relation id is unique, but table name is not
     * @param name table name
     * @return relation id
     */
    public RelationId findRelationIdAndTableName(String name) {
        for (Pair<RelationId, String> pair : relationIdAndTableName) {
            if (pair.second.equals(name)) {
                return pair.first;
            }
        }
        return null;
    }

    private Optional<String> hasSameName() {
        Set<String> tableSet = Sets.newHashSet();
        for (String table : tablelist) {
            if (!tableSet.add(table)) {
                return Optional.of(table);
            }
        }
        return Optional.empty();
    }

    public Map<ExprId, String> getExprIdToTableNameMap() {
        return exprIdToTableNameMap;
    }

    public List<FilterEntry> getFilters() {
        return filters;
    }

    /** Add a filter occurrence with its original join type. The type is stored per
     *  occurrence so that makeJoinPlan can reject conditions whose original type
     *  is incompatible with the generated join, putting them back for later use. */
    public void addFilter(long bitmap, Expression expr, JoinType originalType) {
        filters.add(new FilterEntry(bitmap, expr, originalType));
    }

    /** Check whether a filter originally collected from a join of {@code filterType}
     *  can legally be consumed as a condition of a join of {@code joinType}. */
    public static boolean isJoinTypeCompatible(JoinType filterType, JoinType joinType) {
        return filterType.equals(joinType)
                || filterType.isOneSideOuterJoin() && joinType.isOneSideOuterJoin()
                || filterType.isSemiJoin() && joinType.isSemiJoin()
                || filterType.isAntiJoin() && joinType.isAntiJoin();
    }

    public List<JoinConstraint> getJoinConstraintList() {
        return joinConstraintList;
    }

    public Long getInnerJoinBitmap() {
        return innerJoinBitmap;
    }

    public void setInnerJoinBitmap(Long innerJoinBitmap) {
        this.innerJoinBitmap = innerJoinBitmap;
    }

    public Long getTotalBitmap() {
        return totalBitmap;
    }

    /**
     * set total bitmap used in leading before we get into leading join
     */
    public void setTotalBitmap(Set<RelationId> inputRelationSets) {
        Long totalBitmap = 0L;
        Optional<String> duplicateTableName = hasSameName();
        if (duplicateTableName.isPresent()) {
            this.setStatus(HintStatus.SYNTAX_ERROR);
            this.setErrorMessage("duplicated table:" + duplicateTableName.get());
        }
        Set<RelationId> existRelationSets = new HashSet<>();
        for (int index = 0; index < getTablelist().size(); index++) {
            RelationId id = findRelationIdAndTableName(getTablelist().get(index));
            if (id == null) {
                this.setStatus(HintStatus.SYNTAX_ERROR);
                this.setErrorMessage("can not find table: " + getTablelist().get(index));
                return;
            }
            existRelationSets.add(id);
            totalBitmap = LongBitmap.set(totalBitmap, id.asInt());
        }
        if (getTablelist().size() < inputRelationSets.size()) {
            Set<RelationId> missRelationIds = new HashSet<>();
            missRelationIds.addAll(inputRelationSets);
            missRelationIds.removeAll(existRelationSets);
            String missingTablenames = getMissingTableNames(missRelationIds);
            this.setStatus(HintStatus.SYNTAX_ERROR);
            this.setErrorMessage("leading should have all tables in query block, missing tables: " + missingTablenames);
        }
        this.totalBitmap = totalBitmap;
    }

    private String getMissingTableNames(Set<RelationId> missRelationIds) {
        String missTableNames = "";
        for (RelationId id : missRelationIds) {
            for (Pair<RelationId, String> pair : relationIdAndTableName) {
                if (pair.first.equals(id)) {
                    missTableNames += pair.second + " ";
                }
            }
        }
        return missTableNames;
    }

    /**
     * try to get join constraint, if can not get, it means join is inner join,
     * @param joinTableBitmap table bitmap below this join
     * @param leftTableBitmap table bitmap below right child
     * @param rightTableBitmap table bitmap below right child
     * @return boolean value used for judging whether the join is legal, and should this join need to reverse
     */
    public Pair<JoinConstraint, Boolean> getJoinConstraint(Long joinTableBitmap, Long leftTableBitmap,
                                                           Long rightTableBitmap) {
        boolean reversed = false;
        boolean mustBeLeftjoin = false;

        JoinConstraint matchedJoinConstraint = null;

        for (JoinConstraint joinConstraint : joinConstraintList) {
            if (joinConstraint.getJoinType().isFullOuterJoin()) {
                if (leftTableBitmap.equals(joinConstraint.getLeftHand())
                        && rightTableBitmap.equals(joinConstraint.getRightHand())
                        || rightTableBitmap.equals(joinConstraint.getLeftHand())
                        && leftTableBitmap.equals(joinConstraint.getRightHand())) {
                    if (matchedJoinConstraint != null) {
                        return Pair.of(null, false);
                    }
                    matchedJoinConstraint = joinConstraint;
                    reversed = false;
                    break;
                } else {
                    continue;
                }
            }

            if (!LongBitmap.isOverlap(joinConstraint.getMinRightHand(), joinTableBitmap)) {
                continue;
            }

            if (LongBitmap.isSubset(joinTableBitmap, joinConstraint.getMinRightHand())) {
                continue;
            }

            if (LongBitmap.isSubset(joinConstraint.getMinLeftHand(), leftTableBitmap)
                    && LongBitmap.isSubset(joinConstraint.getMinRightHand(), leftTableBitmap)) {
                continue;
            }

            if (LongBitmap.isSubset(joinConstraint.getMinLeftHand(), rightTableBitmap)
                    && LongBitmap.isSubset(joinConstraint.getMinRightHand(), rightTableBitmap)) {
                continue;
            }

            // Semi/anti join constraints require the constrained side (the right
            // table for left semi/anti, or the left table for right semi/anti)
            // to remain intact on one child — it cannot be split across children
            // or mixed with extra tables. The first guard (isSubset
            // minLeftHand/minRightHand) ensures the constraint is only evaluated
            // when all required tables are present in the current join. Once
            // present, if the constrained side is violated, the leading hint can
            // never satisfy this constraint (tables only merge, never split), so
            // we must return failure immediately.
            //
            // Example 1 — LEFT SEMI JOIN where the constrained side gets mixed:
            //   SELECT /*+ leading(a2 a5 { a3 a1 }) */ ...
            //   FROM a1 JOIN a2 ON ...
            //   LEFT SEMI JOIN a3 ON a2.c2 = a2.c1
            //   JOIN a5 ON a2.c2 = a2.c1;
            // Original tree: ((a1 JOIN a2) LEFT SEMI JOIN a3) JOIN a5
            // Leading asks:  ((a2 JOIN a5) JOIN (a3 JOIN a1))
            // At the top join: left={a2,a5}, right={a1,a3}
            // Constrained side (right table a3 for LEFT SEMI) is mixed with
            // a1 on the right child → violated, leading must be UNUSED.
            //
            // Example 2 — RIGHT ANTI JOIN where constrained side gets mixed:
            //   SELECT /*+ leading(a3 a1 a2) */ ...
            //   FROM a1 RIGHT ANTI JOIN a2 ON a1.c4 = a1.c
            //   JOIN a3 ON a2.c = a3.c3;
            // Original tree: (a1 RIGHT ANTI JOIN a2) JOIN a3
            // Leading asks:  ((a3 JOIN a1) JOIN a2)
            // At the top join: left={a1,a3}, right={a2}
            // Constrained side (left table a1 for RIGHT ANTI) is mixed with
            // a3 → violated, leading must be UNUSED.
            if (joinConstraint.getJoinType().isSemiOrAntiJoin()) {
                if (!LongBitmap.isSubset(joinConstraint.getMinLeftHand(), joinTableBitmap)
                        || !LongBitmap.isSubset(joinConstraint.getMinRightHand(), joinTableBitmap)) {
                    continue;
                }

                Long constrainedSide = joinConstraint.getJoinType().isRightSemiOrAntiJoin()
                        ? joinConstraint.getLeftHand() : joinConstraint.getRightHand();
                if (LongBitmap.isOverlap(constrainedSide, leftTableBitmap)
                        && !constrainedSide.equals(leftTableBitmap)) {
                    return Pair.of(null, false);
                }
                if (LongBitmap.isOverlap(constrainedSide, rightTableBitmap)
                        && !constrainedSide.equals(rightTableBitmap)) {
                    return Pair.of(null, false);
                }
            }

            if (LongBitmap.isSubset(joinConstraint.getMinLeftHand(), leftTableBitmap)
                    && LongBitmap.isSubset(joinConstraint.getMinRightHand(), rightTableBitmap)) {
                if (matchedJoinConstraint != null) {
                    return Pair.of(null, false);
                }
                matchedJoinConstraint = joinConstraint;
                reversed = false;
            } else if (LongBitmap.isSubset(joinConstraint.getMinLeftHand(), rightTableBitmap)
                    && LongBitmap.isSubset(joinConstraint.getMinRightHand(), leftTableBitmap)) {
                if (matchedJoinConstraint != null) {
                    return Pair.of(null, false);
                }
                matchedJoinConstraint = joinConstraint;
                reversed = true;
            } else if (joinConstraint.getJoinType().isSemiJoin()
                    && joinConstraint.getRightHand().equals(rightTableBitmap)) {
                if (matchedJoinConstraint != null) {
                    return Pair.of(null, false);
                }
                matchedJoinConstraint = joinConstraint;
                reversed = false;
            } else if (joinConstraint.getJoinType().isSemiJoin()
                    && joinConstraint.getRightHand().equals(leftTableBitmap)) {
                /* Reversed semijoin case */
                if (matchedJoinConstraint != null) {
                    return Pair.of(null, false);
                }
                matchedJoinConstraint = joinConstraint;
                reversed = true;
            } else {
                if (LongBitmap.isOverlap(leftTableBitmap, joinConstraint.getMinRightHand())
                        && LongBitmap.isOverlap(rightTableBitmap, joinConstraint.getMinRightHand())) {
                    continue;
                }

                if (!joinConstraint.getJoinType().isLeftJoin()
                        || LongBitmap.isOverlap(joinTableBitmap, joinConstraint.getMinLeftHand())) {
                    return Pair.of(null, false);
                }

                mustBeLeftjoin = true;
            }
        }
        if (mustBeLeftjoin && (matchedJoinConstraint == null || !matchedJoinConstraint.getJoinType().isLeftJoin()
                || !matchedJoinConstraint.isLhsStrict())) {
            return Pair.of(null, false);
        }
        // this means inner join
        if (matchedJoinConstraint == null) {
            return Pair.of(null, true);
        }
        matchedJoinConstraint.setReversed(reversed);
        return Pair.of(matchedJoinConstraint, true);
    }

    /**
     * Try to get join type of two random logical scan or join node table bitmap
     * @param left left side table bitmap
     * @param right right side table bitmap
     * @return join type or failure
     */
    public JoinType computeJoinType(Long left, Long right, List<Expression> conditions) {
        Pair<JoinConstraint, Boolean> joinConstraintBooleanPair
                = getJoinConstraint(LongBitmap.or(left, right), left, right);
        if (!joinConstraintBooleanPair.second) {
            this.setStatus(HintStatus.UNUSED);
        } else if (joinConstraintBooleanPair.first == null) {
            if (conditions.isEmpty()) {
                return JoinType.CROSS_JOIN;
            }
            return JoinType.INNER_JOIN;
        } else {
            JoinConstraint joinConstraint = joinConstraintBooleanPair.first;
            if (joinConstraint.isReversed()) {
                return joinConstraint.getJoinType().swap();
            } else {
                return joinConstraint.getJoinType();
            }
        }
        if (conditions.isEmpty()) {
            return JoinType.CROSS_JOIN;
        }
        return JoinType.INNER_JOIN;
    }

    private LogicalPlan makeJoinPlan(LogicalPlan leftChild, LogicalPlan rightChild, String distributeJoinType) {
        DistributeHint distributeHint = new DistributeHint(DistributeType.NONE);
        if (!distributeJoinType.equals("join")) {
            distributeHint = strToHint.get(distributeJoinType);
        }
        // Collect candidate conditions whose bitmap is covered by this join.
        // Each entry carries the original join type from CollectJoinConstraint.
        List<FilterEntry> candidateEntries = collectJoinConditions(
                getFilters(), leftChild, rightChild);
        List<Expression> candidateExprs = new ArrayList<>();
        for (FilterEntry e : candidateEntries) {
            candidateExprs.add(e.expr);
        }
        // Determine join type using all candidate expressions for constraint matching.
        JoinType joinType = computeJoinType(getBitmap(leftChild),
                getBitmap(rightChild), candidateExprs);
        if (joinType == null) {
            // Put back all candidates since we cannot build this join.
            filters.addAll(candidateEntries);
            this.setStatus(HintStatus.SYNTAX_ERROR);
            this.setErrorMessage("JoinType can not be null");
            return null;
        }
        // Keep only entries whose original type is compatible with the generated
        // join type. Incompatible entries are returned to the global filter list
        // so they can be consumed at a later (higher) join level.
        List<Expression> conditions = new ArrayList<>();
        for (FilterEntry entry : candidateEntries) {
            if (isJoinTypeCompatible(entry.originalType, joinType)) {
                conditions.add(entry.expr);
            } else {
                filters.add(entry);
            }
        }
        // Rejected candidates must not shape the join type: if all compatible
        // conditions were filtered out, demote INNER_JOIN to CROSS_JOIN.
        if (conditions.isEmpty() && joinType == JoinType.INNER_JOIN) {
            joinType = JoinType.CROSS_JOIN;
        }
        if (!this.isSuccess()) {
            return null;
        }
        Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                leftChild.getOutput(), rightChild.getOutput(), conditions);
        // get joinType
        LogicalJoin logicalJoin = new LogicalJoin<>(joinType, pair.first,
                pair.second,
                distributeHint,
                Optional.empty(),
                leftChild,
                rightChild, null);
        logicalJoin.getJoinReorderContext().setLeadingJoin(true);
        logicalJoin.setBitmap(LongBitmap.or(getBitmap(leftChild), getBitmap(rightChild)));
        return logicalJoin;
    }

    /**
     * using leading to generate plan, it could be failed, if failed set leading status to unused or syntax error
     * @return plan
     */
    public Plan generateLeadingJoinPlan() {
        Stack<LogicalPlan> stack = new Stack<>();
        for (String item : normalizedParameters) {
            if (item.equals("join") || item.startsWith("shuffle") || item.startsWith("broadcast")) {
                LogicalPlan rightChild = stack.pop();
                LogicalPlan leftChild = stack.pop();
                LogicalPlan joinPlan = makeJoinPlan(leftChild, rightChild, item);
                if (joinPlan == null) {
                    return null;
                }
                stack.push(joinPlan);
            } else {
                LogicalPlan logicalPlan = getLogicalPlanByName(item);
                logicalPlan = makeFilterPlanIfExist(getFilters(), logicalPlan);
                stack.push(logicalPlan);
            }
        }

        LogicalJoin finalJoin = (LogicalJoin) stack.pop();
        // Any filter that was put back due to incompatible join type but never
        // consumed by a later join means the leading hint cannot preserve the
        // original semantics. Fail unconditionally.
        if (!filters.isEmpty()) {
            this.setStatus(HintStatus.UNUSED);
            this.setErrorMessage("leading plan cannot consume all join predicates, leftover: "
                    + filters);
            return null;
        }
        if (finalJoin != null) {
            this.setStatus(HintStatus.SUCCESS);
        }
        return finalJoin;
    }

    /** Collect filter entries whose bitmap is a subset of the given join's tables.
     *  Removes them from the global filter list; the caller must put back any entry
     *  whose original type is incompatible with the final join type. */
    private List<FilterEntry> collectJoinConditions(List<FilterEntry> filters,
                                                     LogicalPlan left, LogicalPlan right) {
        List<FilterEntry> collected = new ArrayList<>();
        Long tablesBitMap = LongBitmap.or(getBitmap(left), getBitmap(right));
        for (int i = filters.size() - 1; i >= 0; i--) {
            FilterEntry entry = filters.get(i);
            if (LongBitmap.isSubset(entry.bitmap, tablesBitMap)) {
                collected.add(entry);
                filters.remove(i);
            }
        }
        return collected;
    }

    private LogicalPlan makeFilterPlanIfExist(List<FilterEntry> filters, LogicalPlan scan) {
        Set<Expression> newConjuncts = new HashSet<>();
        Long scanBitmap = getBitmap(scan);
        for (int i = filters.size() - 1; i >= 0; i--) {
            FilterEntry entry = filters.get(i);
            if (LongBitmap.isSubset(entry.bitmap, scanBitmap)) {
                newConjuncts.add(entry.expr);
                filters.remove(i);
            }
        }
        if (newConjuncts.isEmpty()) {
            return scan;
        } else {
            return new LogicalFilter<>(newConjuncts, scan);
        }
    }

    private Long getBitmap(LogicalPlan root) {
        if (root instanceof LogicalJoin) {
            return ((LogicalJoin) root).getBitmap();
        } else if (root instanceof LogicalRelation) {
            return LongBitmap.set(0L, (((LogicalRelation) root).getRelationId().asInt()));
        } else if (root instanceof LogicalFilter) {
            return getBitmap((LogicalPlan) root.child(0));
        } else if (root instanceof LogicalProject) {
            return getBitmap((LogicalPlan) root.child(0));
        } else if (root instanceof LogicalSubQueryAlias) {
            return LongBitmap.set(0L, (((LogicalSubQueryAlias) root).getRelationId().asInt()));
        } else {
            return null;
        }
    }
}
