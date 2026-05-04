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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.util.MutableState;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Recompute physical properties bottom-up after post-process rewrites.
 */
public class RecomputePhysicalPropertiesPostProcessor extends PlanPostProcessor {
    public static RecomputePhysicalPropertiesPostProcessor INSTANCE = new RecomputePhysicalPropertiesPostProcessor();

    @Override
    public Plan visit(Plan plan, CascadesContext ctx) {
        AbstractPhysicalPlan rewritten = (AbstractPhysicalPlan) visitChildren(this, plan, ctx);

        List<PhysicalProperties> childrenOutputProperties = collectChildrenOutputProperties(rewritten);
        PhysicalProperties outputPhysicalProperties = rewritten.accept(
                new ChildOutputPropertyDeriver(childrenOutputProperties),
                new PlanContext(ctx.getConnectContext(), null, childrenOutputProperties));

        AbstractPhysicalPlan output = rewritten;
        if (!outputPhysicalProperties.equals(rewritten.getPhysicalProperties())) {
            output = (AbstractPhysicalPlan) rewritten
                    .withPhysicalPropertiesAndStats(outputPhysicalProperties, rewritten.getStats());
        }

        return copyGroupState((AbstractPhysicalPlan) plan, output);
    }

    private List<PhysicalProperties> collectChildrenOutputProperties(AbstractPhysicalPlan plan) {
        ImmutableList.Builder<PhysicalProperties> childrenOutputProperties =
                ImmutableList.builderWithExpectedSize(plan.arity());
        for (Plan child : plan.children()) {
            childrenOutputProperties.add(((AbstractPhysicalPlan) child).getPhysicalProperties());
        }
        return childrenOutputProperties.build();
    }

    private AbstractPhysicalPlan copyGroupState(AbstractPhysicalPlan from, AbstractPhysicalPlan to) {
        Optional<Object> groupState = from.getMutableState(MutableState.KEY_GROUP);
        groupState.ifPresent(state -> to.setMutableState(MutableState.KEY_GROUP, state));
        return to;
    }
}
