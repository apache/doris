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

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.plans.Plan;

import org.apache.commons.lang.StringUtils;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Check analysis rule to check semantic correct after analysis by Nereids.
 */
public class CheckAnalysis extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return any().then(plan -> {
            checkBound(plan);
            checkExpressionInputTypes(plan);
            return null;
        }).toRule(RuleType.CHECK_ANALYSIS);
    }

    private void checkExpressionInputTypes(Plan plan) {
        final Optional<TypeCheckResult> firstFailed = plan.getExpressions().stream()
                .map(Expression::checkInputDataTypes)
                .filter(TypeCheckResult::failed)
                .findFirst();

        if (firstFailed.isPresent()) {
            throw new AnalysisException(firstFailed.get().getMessage());
        }
    }

    private void checkBound(Plan plan) {
        Set<UnboundSlot> unboundSlots = plan.getExpressions().stream()
                .<Set<UnboundSlot>>map(e -> e.collect(UnboundSlot.class::isInstance))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        if (!unboundSlots.isEmpty()) {
            throw new AnalysisException(String.format("Cannot find column %s.",
                    StringUtils.join(unboundSlots.stream()
                            .map(UnboundSlot::toSql)
                            .collect(Collectors.toSet()), ", ")));
        }
    }
}
