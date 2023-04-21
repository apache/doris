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
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;

import java.util.List;
import java.util.stream.Collectors;

/**
 * add redistribute node to compatible to the key of insert into target.
 */
public class AddDistributionSpecForInsertIntoCommand extends PlanPostProcessor {
    @Override
    public Plan visit(Plan plan, CascadesContext ctx) {
        // add a physical distribution to insert into source relation
        List<ExprId> keys = plan.getOutput()
                .subList(0, ctx.getStatementContext().getInsertIntoContext().getKeyNums())
                .stream().map(NamedExpression::getExprId).collect(Collectors.toList());
        return new PhysicalDistribute<>(
                new DistributionSpecHash(keys, ShuffleType.NATURAL),
                plan.getLogicalProperties(),
                plan
        );
    }
}
