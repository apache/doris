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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;

import com.google.common.collect.ImmutableList;

import java.util.List;

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
        if (logicalPartitionTopN.getPartitionKeys().isEmpty()) {
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
