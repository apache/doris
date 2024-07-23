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
import org.apache.doris.qe.ConnectContext;

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

    private final Map<Integer, DistributeHint> distributeHints = new HashMap<>();

    private final Map<RelationId, LogicalPlan> relationIdToScanMap = Maps.newLinkedHashMap();

    private final List<Pair<RelationId, String>> relationIdAndTableName = new ArrayList<>();

    private final Map<ExprId, String> exprIdToTableNameMap = Maps.newLinkedHashMap();

    private final List<Pair<Long, Expression>> filters = new ArrayList<>();

    private final Map<Expression, JoinType> conditionJoinType = Maps.newLinkedHashMap();

    private final List<JoinConstraint> joinConstraintList = new ArrayList<>();

    private Long innerJoinBitmap = 0L;

    private Long totalBitmap = 0L;

    public LeadingHint(String hintName) {
        super(hintName);
    }

    /**
     * Leading hint data structure before using
     * @param hintName Leading
     * @param parameters table name mixed with left and right brace
     */
    public LeadingHint(String hintName, List<String> parameters, String originalString) {
        super(hintName);
        this.originalString = originalString;
        addJoinParameters = insertJoinIntoParameters(parameters);
        normalizedParameters = parseIntoReversePolishNotation(addJoinParameters);
    }

    /**
     * insert join string into leading string
     * @param list of sql input leading string
     * @return list of string adding joins into tables
     */
    public static List<String> insertJoinIntoParameters(List<String> list) {
        List<String> output = new ArrayList<>();

        for (String item : list) {
            if (item.equals("shuffle") || item.equals("broadcast")) {
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
            if (!(item.equals("shuffle") || item.equals("broadcast") || item.equals("{")
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
                if (item.equals("shuffle")) {
                    distributeHints.put(item.hashCode(), new DistributeHint(DistributeType.SHUFFLE_RIGHT));
                } else if (item.equals("broadcast")) {
                    distributeHints.put(item.hashCode(), new DistributeHint(DistributeType.BROADCAST_RIGHT));
                }

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
            } else if (parameter.equals("shuffle") || parameter.equals("broadcast")) {
                DistributeHint distributeHint = distributeHints.get(parameter.hashCode());
                if (distributeHint.isSuccess()) {
                    out.append(parameter + " ");
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

    public List<Pair<Long, Expression>> getFilters() {
        return filters;
    }

    public void putConditionJoinType(Expression filter, JoinType joinType) {
        conditionJoinType.put(filter, joinType);
    }

    /**
     * find out whether conditions can match original joinType
     * @param conditions conditions needs to put on this join
     * @param joinType join type computed by join constraint
     * @return can conditions matched
     */
    public boolean isConditionJoinTypeMatched(List<Expression> conditions, JoinType joinType) {
        for (Expression condition : conditions) {
            JoinType originalJoinType = conditionJoinType.get(condition);
            if (originalJoinType.equals(joinType)
                    || originalJoinType.isOneSideOuterJoin() && joinType.isOneSideOuterJoin()
                    || originalJoinType.isSemiJoin() && joinType.isSemiJoin()
                    || originalJoinType.isAntiJoin() && joinType.isAntiJoin()) {
                continue;
            }
            return false;
        }
        return true;
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

            if (joinConstraint.getJoinType().isSemiJoin()) {
                if (LongBitmap.isSubset(joinConstraint.getRightHand(), leftTableBitmap)
                        && !LongBitmap.isSubset(joinConstraint.getRightHand(), leftTableBitmap)) {
                    continue;
                }
                if (LongBitmap.isSubset(joinConstraint.getRightHand(), rightTableBitmap)
                        && !joinConstraint.getRightHand().equals(rightTableBitmap)) {
                    continue;
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

    private DistributeHint getDistributeJoinHint(String distributeJoinType) {
        DistributeHint distributeHint = null;
        if (distributeJoinType.equals("join")) {
            distributeHint = new DistributeHint(DistributeType.NONE);
        } else if (distributeJoinType.equals("shuffle") || distributeJoinType.equals("broadcast")) {
            distributeHint = distributeHints.get(distributeJoinType.hashCode());
        }
        distributeHint.setSuccessInLeading(true);
        if (!ConnectContext.get().getStatementContext().getHints().contains(distributeHint)) {
            ConnectContext.get().getStatementContext().addHint(distributeHint);
        }
        distributeHints.put(0, distributeHint);
        return distributeHint;
    }

    private LogicalPlan makeJoinPlan(LogicalPlan leftChild, LogicalPlan rightChild, String distributeJoinType) {
        List<Expression> conditions = getJoinConditions(
                getFilters(), leftChild, rightChild);
        Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                leftChild.getOutput(), rightChild.getOutput(), conditions);
        // leading hint would set status inside if not success
        JoinType joinType = computeJoinType(getBitmap(leftChild),
                getBitmap(rightChild), conditions);
        if (joinType == null) {
            this.setStatus(HintStatus.SYNTAX_ERROR);
            this.setErrorMessage("JoinType can not be null");
        } else if (!isConditionJoinTypeMatched(conditions, joinType)) {
            this.setStatus(HintStatus.UNUSED);
            this.setErrorMessage("condition does not matched joinType");
        }
        if (!this.isSuccess()) {
            return null;
        }
        // get joinType
        DistributeHint distributeHint = getDistributeJoinHint(distributeJoinType);
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
            if (item.equals("join") || item.equals("shuffle") || item.equals("broadcast")) {
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
        // we want all filters been remove
        assert (filters.isEmpty());
        if (finalJoin != null) {
            this.setStatus(HintStatus.SUCCESS);
        }
        return finalJoin;
    }

    private DistributeHint getJoinHint(Integer index) {
        if (distributeHints.get(index) == null) {
            return new DistributeHint(DistributeType.NONE);
        }
        distributeHints.get(index).setSuccessInLeading(true);
        return distributeHints.get(index);
    }

    private List<Expression> getJoinConditions(List<Pair<Long, Expression>> filters,
                                               LogicalPlan left, LogicalPlan right) {
        List<Expression> joinConditions = new ArrayList<>();
        for (int i = filters.size() - 1; i >= 0; i--) {
            Pair<Long, Expression> filterPair = filters.get(i);
            Long tablesBitMap = LongBitmap.or(getBitmap(left), getBitmap(right));
            // left one is smaller set
            if (LongBitmap.isSubset(filterPair.first, tablesBitMap)) {
                joinConditions.add(filterPair.second);
                filters.remove(i);
            }
        }
        return joinConditions;
    }

    private List<Expression> getLastConditions(List<Pair<Long, Expression>> filters) {
        List<Expression> joinConditions = new ArrayList<>();
        for (int i = filters.size() - 1; i >= 0; i--) {
            Pair<Long, Expression> filterPair = filters.get(i);
            joinConditions.add(filterPair.second);
            filters.remove(i);
        }
        return joinConditions;
    }

    private LogicalPlan makeFilterPlanIfExist(List<Pair<Long, Expression>> filters, LogicalPlan scan) {
        Set<Expression> newConjuncts = new HashSet<>();
        for (int i = filters.size() - 1; i >= 0; i--) {
            Pair<Long, Expression> filterPair = filters.get(i);
            if (LongBitmap.isSubset(filterPair.first, getBitmap(scan))) {
                newConjuncts.add(filterPair.second);
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
