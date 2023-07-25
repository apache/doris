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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Maps;
import org.apache.doris.nereids.util.JoinUtils;

import java.util.ArrayList;
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
    // e.g. query_timeout='1800', exec_mem_limit='2147483648'
    private final List<String> tablelist = new ArrayList<>();
    private final List<Integer> levellist = new ArrayList<>();

    private final Map<String, LogicalPlan> tableNameToScanMap = Maps.newLinkedHashMap();

    private final List<Pair<Long, Expression>> filters = new ArrayList<>();

    private final List<JoinConstraint> joinConstraintList = new ArrayList<>();

    private Long innerJoinBitmap = 0L;

    public LeadingHint(String hintName) {
        super(hintName);
    }

    /**
     * Leading hint data structure before using
     * @param hintName Leading
     * @param parameters table name mixed with left and right brace
     */
    public LeadingHint(String hintName, List<String> parameters) {
        super(hintName);
        int level = 0;
        for (String parameter : parameters) {
            if (parameter.equals("{")) {
                ++level;
            } else if (parameter.equals("}")) {
                level--;
            } else {
                tablelist.add(parameter);
                levellist.add(level);
            }
        }
    }

    public List<String> getTablelist() {
        return tablelist;
    }

    public List<Integer> getLevellist() {
        return levellist;
    }

    public Map<String, LogicalPlan> getTableNameToScanMap() {
        return tableNameToScanMap;
    }

    public List<Pair<Long, Expression>> getFilters() {
        return filters;
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
    public JoinType computeJoinType(Long left, Long right) {
        Pair<JoinConstraint, Boolean> joinConstraintBooleanPair
                = getJoinConstraint(LongBitmap.or(left, right), left, right);
        if (!joinConstraintBooleanPair.second) {
            assert (1 != 1);
            //throw exception
        } else if (joinConstraintBooleanPair.first == null) {
            return JoinType.INNER_JOIN;
        } else {
            JoinConstraint joinConstraint = joinConstraintBooleanPair.first;
            if (joinConstraint.isReversed()) {
                return joinConstraint.getJoinType().swap();
            } else {
                return joinConstraint.getJoinType();
            }
        }
        return JoinType.INNER_JOIN;
    }

    /**
     * using leading to generate plan, it could be failed, if failed set leading status to unused or syntax error
     * @return plan
     */
    public Plan generateLeadingJoinPlan() {
        Stack<Pair<Integer, LogicalPlan>> stack = new Stack<>();
        int index = 0;
        LogicalPlan logicalPlan = getTableNameToScanMap().get(getTablelist().get(index));
        logicalPlan = makeFilterPlanIfExist(getFilters(), logicalPlan);
        assert (logicalPlan != null);
        stack.push(Pair.of(getLevellist().get(index), logicalPlan));
        int stackTopLevel = getLevellist().get(index++);
        while (index < getTablelist().size()) {
            int currentLevel = getLevellist().get(index);
            if (currentLevel == stackTopLevel) {
                // should return error if can not found table
                logicalPlan = getTableNameToScanMap().get(getTablelist().get(index++));
                logicalPlan = makeFilterPlanIfExist(getFilters(), logicalPlan);
                Pair<Integer, LogicalPlan> newStackTop = stack.peek();
                while (!(stack.isEmpty() || stackTopLevel != newStackTop.first)) {
                    // check join is legal and get join type
                    newStackTop = stack.pop();
                    List<Expression> conditions = getJoinConditions(
                            getFilters(), newStackTop.second, logicalPlan);
                    Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                        newStackTop.second.getOutput(), logicalPlan.getOutput(), conditions);
                    JoinType joinType = computeJoinType(getBitmap(newStackTop.second), getBitmap(logicalPlan));
                    // get joinType
                    LogicalJoin logicalJoin = new LogicalJoin<>(joinType, pair.first,
                            pair.second,
                            JoinHint.NONE,
                            Optional.empty(),
                            newStackTop.second,
                            logicalPlan);
                    if (stackTopLevel > 0) {
                        stackTopLevel--;
                    }
                    if (!stack.isEmpty()) {
                        newStackTop = stack.peek();
                    }
                    logicalPlan = logicalJoin;
                }
                stack.push(Pair.of(stackTopLevel, logicalPlan));
            } else {
                // push
                logicalPlan = getTableNameToScanMap().get(getTablelist().get(index++));
                stack.push(Pair.of(currentLevel, logicalPlan));
                stackTopLevel = currentLevel;
            }
        }

        // we want all filters been remove
        assert (getFilters().isEmpty());
        return stack.pop().second;
    }

    private List<Expression> getJoinConditions(List<Pair<Long, Expression>> filters,
                                               LogicalPlan left, LogicalPlan right) {
        List<Expression> joinConditions = new ArrayList<>();
        for (int i = filters.size() - 1; i >= 0; i--) {
            Pair<Long, Expression> filterPair = filters.get(i);
            Long tablesBitMap = LongBitmap.or(getBitmap(left), getBitmap(right));
            if (LongBitmap.isSubset(filterPair.first, tablesBitMap)) {
                joinConditions.add(filterPair.second);
                filters.remove(i);
            }
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
        } else {
            return null;
        }
    }

    /**
     * get leading containing tables which means leading wants to combine tables into joins
     * @return long value represent tables we included
     */
    public Long getLeadingTableBitmap() {
        Long totalBitmap = 0L;
        for (int index = 0; index < getTablelist().size(); index++) {
            LogicalPlan logicalPlan = getTableNameToScanMap().get(getTablelist().get(index));
            totalBitmap = LongBitmap.set(totalBitmap, (((LogicalRelation) logicalPlan).getRelationId().asInt()));
        }
        return totalBitmap;
    }

    public Long computeTableBitmap(Set<RelationId> relationIdSet) {
        Long totalBitmap = 0L;
        for (RelationId id : relationIdSet) {
            totalBitmap = LongBitmap.set(totalBitmap, (id.asInt()));
        }
        return totalBitmap;
    }
}
