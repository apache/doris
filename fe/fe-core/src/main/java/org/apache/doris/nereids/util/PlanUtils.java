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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.List;
import java.util.Optional;

/**
 * Util for plan
 */
public class PlanUtils {
    public static Optional<LogicalProject<? extends Plan>> project(List<NamedExpression> projectExprs, Plan plan) {
        if (projectExprs.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new LogicalProject<>(projectExprs, plan));
    }

    public static Plan projectOrSelf(List<NamedExpression> projectExprs, Plan plan) {
        return project(projectExprs, plan).map(Plan.class::cast).orElse(plan);
    }

    public static Optional<LogicalFilter<? extends Plan>> filter(List<Expression> predicates, Plan plan) {
        return ExpressionUtils.optionalAnd(predicates)
                .map(predicate -> new LogicalFilter<>(predicate, plan));
    }

    public static Plan filterOrSelf(List<Expression> predicates, Plan plan) {
        return filter(predicates, plan).map(Plan.class::cast).orElse(plan);
    }
}
