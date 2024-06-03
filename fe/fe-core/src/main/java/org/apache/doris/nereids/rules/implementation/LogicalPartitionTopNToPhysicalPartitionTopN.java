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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Implementation rule that convert logical partition-top-n to physical partition-top-n.
 */
public class LogicalPartitionTopNToPhysicalPartitionTopN extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalPartitionTopN().thenApplyMulti(ctx -> generatePhysicalPartitionTopn(ctx.root))
                .toRule(RuleType.LOGICAL_PARTITION_TOP_N_TO_PHYSICAL_PARTITION_TOP_N_RULE);
    }

    private List<PhysicalPartitionTopN<? extends Plan>> generatePhysicalPartitionTopn(
            LogicalPartitionTopN<? extends Plan> logicalPartitionTopN) {
        if (logicalPartitionTopN.getPartitionKeys().isEmpty()
                || !checkTwoPhaseGlobalPartitionTopn(logicalPartitionTopN)) {
            // if no partition by keys, use local partition topn combined with further full sort
            List<OrderKey> orderKeys = !logicalPartitionTopN.getOrderKeys().isEmpty()
                    ? logicalPartitionTopN.getOrderKeys().stream()
                    .map(OrderExpression::getOrderKey)
                    .collect(ImmutableList.toImmutableList()) :
                    ImmutableList.of();

            PhysicalPartitionTopN<Plan> onePhaseLocalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    orderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    logicalPartitionTopN.child(0));

            return ImmutableList.of(onePhaseLocalPartitionTopN);
        } else {
            // if partition by keys exist, the order keys will be set as original partition keys combined with
            // orderby keys, to meet upper window operator's order requirement.
            ImmutableList<OrderKey> fullOrderKeys = getAllOrderKeys(logicalPartitionTopN);
            PhysicalPartitionTopN<Plan> onePhaseGlobalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    fullOrderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.ONE_PHASE_GLOBAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    logicalPartitionTopN.child(0));

            PhysicalPartitionTopN<Plan> twoPhaseLocalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    fullOrderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    logicalPartitionTopN.child(0));

            PhysicalPartitionTopN<Plan> twoPhaseGlobalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    fullOrderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    twoPhaseLocalPartitionTopN);

            return ImmutableList.of(onePhaseGlobalPartitionTopN, twoPhaseGlobalPartitionTopN);
        }
    }

    /**
     * check if partition keys' ndv is almost near the total row count.
     * if yes, it is not suitable for two phase global partition topn.
     */
    private boolean checkTwoPhaseGlobalPartitionTopn(LogicalPartitionTopN<? extends Plan> logicalPartitionTopN) {
        double globalPartitionTopnThreshold = ConnectContext.get().getSessionVariable()
                .getGlobalPartitionTopNThreshold();
        if (logicalPartitionTopN.getGroupExpression().isPresent()) {
            Group group = logicalPartitionTopN.getGroupExpression().get().getOwnerGroup();
            if (group != null && group.getStatistics() != null) {
                Statistics stats = group.getStatistics();
                double rowCount = stats.getRowCount();
                List<Expression> partitionKeys = logicalPartitionTopN.getPartitionKeys();
                if (!checkPartitionKeys(partitionKeys)) {
                    return false;
                }
                List<ColumnStatistic> partitionByKeyStats = partitionKeys.stream()
                        .map(partitionKey -> stats.findColumnStatistics(partitionKey))
                        .filter(Objects::nonNull)
                        .filter(e -> !e.isUnKnown)
                        .collect(Collectors.toList());
                if (partitionByKeyStats.size() != partitionKeys.size()) {
                    return false;
                } else {
                    List<Double> ndvs = partitionByKeyStats.stream().map(s -> s.ndv)
                            .filter(e -> e > 0 && !Double.isInfinite(e))
                            .collect(Collectors.toList());
                    if (ndvs.size() != partitionByKeyStats.size()) {
                        return false;
                    } else {
                        double maxNdv = ndvs.stream().max(Double::compare).get();
                        return rowCount / maxNdv >= globalPartitionTopnThreshold;
                    }
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * global partition topn only take effect if partition keys are columns from basic table
     */
    private boolean checkPartitionKeys(List<Expression> partitionKeys) {
        for (Expression expr : partitionKeys) {
            if (!(expr instanceof SlotReference)) {
                return false;
            } else {
                SlotReference slot = (SlotReference) expr;
                if (!slot.getColumn().isPresent() || !slot.getTable().isPresent()) {
                    return false;
                }
            }
        }
        return true;
    }

    private ImmutableList<OrderKey> getAllOrderKeys(LogicalPartitionTopN<? extends Plan> logicalPartitionTopN) {
        ImmutableList.Builder<OrderKey> builder = ImmutableList.builder();

        if (!logicalPartitionTopN.getPartitionKeys().isEmpty()) {
            builder.addAll(logicalPartitionTopN.getPartitionKeys().stream().map(partitionKey -> {
                return new OrderKey(partitionKey, true, false);
            }).collect(ImmutableList.toImmutableList()));
        }

        if (!logicalPartitionTopN.getOrderKeys().isEmpty()) {
            builder.addAll(logicalPartitionTopN.getOrderKeys().stream()
                    .map(OrderExpression::getOrderKey)
                    .collect(ImmutableList.toImmutableList())
            );
        }

        return builder.build();
    }
}
