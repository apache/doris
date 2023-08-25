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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Abstract class for all concrete physical plan.
 */
public abstract class AbstractPhysicalPlan extends AbstractPlan implements PhysicalPlan, Explainable {

    protected final PhysicalProperties physicalProperties;

    public AbstractPhysicalPlan(PlanType type, LogicalProperties logicalProperties, Plan... children) {
        this(type, Optional.empty(), logicalProperties, children);
    }

    public AbstractPhysicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Plan... children) {
        this(type, groupExpression, logicalProperties, PhysicalProperties.ANY, null, children);
    }

    public AbstractPhysicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, @Nullable PhysicalProperties physicalProperties,
            Statistics statistics, Plan... children) {
        super(type, groupExpression,
                logicalProperties == null ? Optional.empty() : Optional.of(logicalProperties),
                statistics, ImmutableList.copyOf(children));
        this.physicalProperties =
                physicalProperties == null ? PhysicalProperties.ANY : physicalProperties;
    }

    public PhysicalProperties getPhysicalProperties() {
        return physicalProperties;
    }

    /**
     * Pushing down runtime filter into different plan node, such as olap scan node, cte sender node, etc.
     */
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
                                         AbstractPhysicalJoin builderNode,
                                         Expression src, Expression probeExpr,
                                         TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        // currently, we can ensure children in the two side are corresponding to the equal_to's.
        // so right maybe an expression and left is a slot
        Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);

        // aliasTransMap doesn't contain the key, means that the path from the olap scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!RuntimeFilterGenerator.checkPushDownPreconditions(builderNode, ctx, probeSlot)) {
            return false;
        }

        boolean pushedDown = false;
        for (Object child : children) {
            AbstractPhysicalPlan childPlan = (AbstractPhysicalPlan) child;
            pushedDown |= childPlan.pushDownRuntimeFilter(context, generator, builderNode, src, probeExpr,
                    type, buildSideNdv, exprOrder);
        }
        if (pushedDown) {
            return true;
        }

        Slot olapScanSlot = aliasTransferMap.get(probeSlot).second;
        PhysicalRelation scan = aliasTransferMap.get(probeSlot).first;
        if (!RuntimeFilterGenerator.isCoveredByPlanNode(this, scan)) {
            return false;
        }
        Preconditions.checkState(olapScanSlot != null && scan != null);

        // in-filter is not friendly to pipeline
        if (type == TRuntimeFilterType.IN_OR_BLOOM
                && ctx.getSessionVariable().getEnablePipelineEngine()
                && RuntimeFilterGenerator.hasRemoteTarget(builderNode, scan)) {
            type = TRuntimeFilterType.BLOOM;
        }
        org.apache.doris.nereids.trees.plans.physical.RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                src, ImmutableList.of(olapScanSlot), type, exprOrder, builderNode, buildSideNdv);
        ctx.addJoinToTargetMap(builderNode, olapScanSlot.getExprId());
        ctx.setTargetExprIdToFilter(olapScanSlot.getExprId(), filter);
        ctx.setTargetsOnScanNode(aliasTransferMap.get(probeExpr).first.getRelationId(), olapScanSlot);
        return true;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this;
    }
}
