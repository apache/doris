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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterGenerator.FilterSizeLimits;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostprocessor {

    private static final IdGenerator<RuntimeFilterId> GENERATOR = RuntimeFilterId.createGenerator();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<Long, List<RuntimeFilter>> filters = Maps.newHashMap();

    private final SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();

    private final FilterSizeLimits limits = new FilterSizeLimits(sessionVariable);

    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> join, CascadesContext ctx) {
        Plan left = join.left();
        Plan right = join.right();
        if (join.getJoinType() == JoinType.INNER_JOIN && join.getCondition().isPresent()) {
            Expression cond = join.getCondition().get();
            List<EqualTo> eqPreds = cond.collect(expr ->
                    expr instanceof EqualTo && !((EqualTo) expr).nullable());
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values()).filter(type ->
                    (type.getValue() & sessionVariable.getRuntimeFilterType()) > 0).collect(Collectors.toList());
            List<RuntimeFilter> runtimeFilters = Lists.newArrayList();
            AtomicInteger cnt = new AtomicInteger();
            final PhysicalHashJoin<Plan, Plan> joinReplica = join;
            eqPreds.forEach(expr -> {
                runtimeFilters.addAll(legalTypes.stream()
                        .map(type -> RuntimeFilter.createRuntimeFilter(GENERATOR.getNextId(), expr,
                                type, cnt.get(), joinReplica, limits))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
                cnt.getAndIncrement();
            });
            left = left.accept(this, ctx);
            runtimeFilters.forEach(RuntimeFilter::setFinalized);
            right = right.accept(this, ctx);
            join = join.withRuntimeFilter(runtimeFilters);
        } else {
            left = join.left().accept(this, ctx);
            right = join.right().accept(this, ctx);
        }
        return join.withChildren(ImmutableList.of(left, right));
    }
}
