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
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Enforcer plan.
 */
public class PhysicalDistribute<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {

    protected DistributionSpec distributionSpec;

    public PhysicalDistribute(DistributionSpec spec, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(spec, Optional.empty(), logicalProperties, child);
    }

    public PhysicalDistribute(DistributionSpec spec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_DISTRIBUTE, groupExpression, logicalProperties, child);
        this.distributionSpec = spec;
    }

    public PhysicalDistribute(DistributionSpec spec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_DISTRIBUTE, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.distributionSpec = spec;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalDistribute[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "distributionSpec", distributionSpec,
                "stats", statistics
        );
    }

    @Override
    public JSONObject toJson() {
        JSONObject physicalDistributeJson = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("DistributionSpec", distributionSpec.toString());
        physicalDistributeJson.put("Properties", properties);
        return physicalDistributeJson;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDistribute(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public PhysicalDistribute<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalDistribute<>(distributionSpec, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, children.get(0));

    }

    @Override
    public PhysicalDistribute<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalDistribute<>(distributionSpec, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalDistribute<>(distributionSpec, groupExpression,
                logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalDistribute<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalDistribute<>(distributionSpec, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
            AbstractPhysicalJoin<?, ?> builderNode, Expression src, Expression probeExpr,
            TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        // currently, we can ensure children in the two side are corresponding to the equal_to's.
        // so right maybe an expression and left is a slot
        Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);

        // aliasTransMap doesn't contain the key, means that the path from the scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!RuntimeFilterGenerator.checkPushDownPreconditionsForJoin(builderNode, ctx, probeSlot)) {
            return false;
        }
        PhysicalRelation scan = aliasTransferMap.get(probeSlot).first;
        if (!RuntimeFilterGenerator.checkPushDownPreconditionsForRelation(this, scan)) {
            return false;
        }
        // TODO: global rf need merge stage which is heavy
        // add some rule, such as bc only is allowed for
        // pushing down through distribute, currently always pushing.
        AbstractPhysicalPlan childPlan = (AbstractPhysicalPlan) child(0);
        return childPlan.pushDownRuntimeFilter(context, generator, builderNode, src, probeExpr,
                type, buildSideNdv, exprOrder);
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalDistribute<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalDistribute<>(distributionSpec, groupExpression,
                null, physicalProperties, statistics, child());
    }
}
