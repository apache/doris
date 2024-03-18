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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.JoinConstraint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 * CollectJoinConstraint
 */
public class CollectJoinConstraint implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalJoin().thenApply(ctx -> {
                if (!ctx.cascadesContext.isLeadingJoin()) {
                    return ctx.root;
                }
                LeadingHint leading = (LeadingHint) ctx.cascadesContext
                            .getHintMap().get("Leading");
                LogicalJoin join = ctx.root;
                if (join.getJoinType().isNullAwareLeftAntiJoin()) {
                    leading.setStatus(Hint.HintStatus.UNUSED);
                    leading.setErrorMessage("condition does not matched joinType");
                }
                Long leftHand = LongBitmap.computeTableBitmap(join.left().getInputRelations());
                Long rightHand = LongBitmap.computeTableBitmap(join.right().getInputRelations());
                join.setBitmap(LongBitmap.or(leftHand, rightHand));
                List<Expression> expressions = join.getHashJoinConjuncts();
                Long totalFilterBitMap = 0L;
                Long nonNullableSlotBitMap = 0L;
                for (Expression expression : expressions) {
                    Long nonNullable = calSlotsTableBitMap(leading, expression.getInputSlots(), true);
                    nonNullableSlotBitMap = LongBitmap.or(nonNullableSlotBitMap, nonNullable);
                    Long filterBitMap = calSlotsTableBitMap(leading, expression.getInputSlots(), false);
                    totalFilterBitMap = LongBitmap.or(totalFilterBitMap, filterBitMap);
                    if (join.getJoinType().isLeftJoin()) {
                        filterBitMap = LongBitmap.or(filterBitMap, rightHand);
                    }
                    leading.getFilters().add(Pair.of(filterBitMap, expression));
                    leading.putConditionJoinType(expression, join.getJoinType());
                }
                expressions = join.getOtherJoinConjuncts();
                for (Expression expression : expressions) {
                    Long nonNullable = calSlotsTableBitMap(leading, expression.getInputSlots(), true);
                    nonNullableSlotBitMap = LongBitmap.or(nonNullableSlotBitMap, nonNullable);
                    Long filterBitMap = calSlotsTableBitMap(leading, expression.getInputSlots(), false);
                    totalFilterBitMap = LongBitmap.or(totalFilterBitMap, filterBitMap);
                    if (join.getJoinType().isLeftJoin()) {
                        filterBitMap = LongBitmap.or(filterBitMap, rightHand);
                    }
                    leading.getFilters().add(Pair.of(filterBitMap, expression));
                    leading.putConditionJoinType(expression, join.getJoinType());
                }
                collectJoinConstraintList(leading, leftHand, rightHand, join, totalFilterBitMap, nonNullableSlotBitMap);

                return ctx.root;
            }).toRule(RuleType.COLLECT_JOIN_CONSTRAINT),

            logicalProject(logicalOlapScan()).thenApply(
                ctx -> {
                    if (!ctx.cascadesContext.isLeadingJoin()) {
                        return ctx.root;
                    }
                    LeadingHint leading = (LeadingHint) ctx.cascadesContext
                            .getHintMap().get("Leading");
                    LogicalProject<LogicalOlapScan> project = ctx.root;
                    LogicalOlapScan scan = project.child();
                    leading.getRelationIdToScanMap().put(scan.getRelationId(), project);
                    return ctx.root;
                }
            ).toRule(RuleType.COLLECT_JOIN_CONSTRAINT)
        );
    }

    private void collectJoinConstraintList(LeadingHint leading, Long leftHand, Long rightHand, LogicalJoin join,
                                            Long filterTableBitMap, Long nonNullableSlotBitMap) {
        Long totalTables = LongBitmap.or(leftHand, rightHand);
        if (join.getJoinType().isInnerOrCrossJoin()) {
            leading.setInnerJoinBitmap(LongBitmap.or(leading.getInnerJoinBitmap(), totalTables));
            return;
        }
        if (join.getJoinType().isFullOuterJoin()) {
            JoinConstraint newJoinConstraint = new JoinConstraint(leftHand, rightHand, leftHand, rightHand,
                    JoinType.FULL_OUTER_JOIN, false);
            leading.getJoinConstraintList().add(newJoinConstraint);
            return;
        }
        boolean isStrict = LongBitmap.isOverlap(nonNullableSlotBitMap, leftHand);
        Long minLeftHand = LongBitmap.newBitmapIntersect(filterTableBitMap, leftHand);
        Long innerJoinTableBitmap = LongBitmap.and(totalTables, leading.getInnerJoinBitmap());
        Long filterAndInnerBelow = LongBitmap.newBitmapUnion(filterTableBitMap, innerJoinTableBitmap);
        Long minRightHand = LongBitmap.newBitmapIntersect(filterAndInnerBelow, rightHand);
        for (JoinConstraint other : leading.getJoinConstraintList()) {
            if (other.getJoinType() == JoinType.FULL_OUTER_JOIN) {
                if (LongBitmap.isOverlap(leftHand, other.getLeftHand())
                        || LongBitmap.isOverlap(leftHand, other.getRightHand())) {
                    minLeftHand = LongBitmap.or(minLeftHand,
                        other.getLeftHand());
                    minLeftHand = LongBitmap.or(minLeftHand,
                        other.getRightHand());
                }
                if (LongBitmap.isOverlap(rightHand, other.getLeftHand())
                        || LongBitmap.isOverlap(rightHand, other.getRightHand())) {
                    minRightHand = LongBitmap.or(minRightHand,
                        other.getLeftHand());
                    minRightHand = LongBitmap.or(minRightHand,
                        other.getRightHand());
                }
                /* Needn't do anything else with the full join */
                continue;
            }

            if (LongBitmap.isOverlap(leftHand, other.getRightHand())) {
                if (LongBitmap.isOverlap(filterTableBitMap, other.getRightHand())
                        && (join.getJoinType().isSemiOrAntiJoin()
                        || !LongBitmap.isOverlap(nonNullableSlotBitMap, other.getMinRightHand()))) {
                    minLeftHand = LongBitmap.or(minLeftHand,
                        other.getLeftHand());
                    minLeftHand = LongBitmap.or(minLeftHand,
                        other.getRightHand());
                }
            }

            if (LongBitmap.isOverlap(rightHand, other.getRightHand())) {
                if (LongBitmap.isOverlap(filterTableBitMap, other.getRightHand())
                        || !LongBitmap.isOverlap(filterTableBitMap, other.getMinLeftHand())
                        || join.getJoinType().isSemiOrAntiJoin()
                        || other.getJoinType().isSemiOrAntiJoin()
                        || !other.isLhsStrict()) {
                    minRightHand = LongBitmap.or(minRightHand, other.getLeftHand());
                    minRightHand = LongBitmap.or(minRightHand, other.getRightHand());
                }
            }
        }
        if (minLeftHand == 0L) {
            minLeftHand = leftHand;
        }
        if (minRightHand == 0L) {
            minRightHand = rightHand;
        }

        JoinConstraint newJoinConstraint = new JoinConstraint(minLeftHand, minRightHand, leftHand, rightHand,
                join.getJoinType(), isStrict);
        leading.getJoinConstraintList().add(newJoinConstraint);
    }

    private long calSlotsTableBitMap(LeadingHint leading, Set<Slot> slots, boolean getNotNullable) {
        Preconditions.checkArgument(slots.size() != 0);
        long bitmap = LongBitmap.newBitmap();
        for (Slot slot : slots) {
            if (getNotNullable && slot.nullable()) {
                continue;
            }
            if (!slot.isColumnFromTable() && (slot.getQualifier() == null || slot.getQualifier().isEmpty())) {
                // we can not get info from column not from table
                continue;
            }
            String tableName = slot.getQualifier().get(slot.getQualifier().size() - 1);
            RelationId id = leading.findRelationIdAndTableName(tableName);
            if (id == null) {
                leading.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                leading.setErrorMessage("can not find table: " + tableName);
                return bitmap;
            }
            long currBitmap = LongBitmap.set(bitmap, id.asInt());
            bitmap = LongBitmap.or(bitmap, currBitmap);
        }
        return bitmap;
    }
}
