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

import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Check bound rule to check semantic correct after bounding of expression by Nereids.
 * Also give operator information without LOGICAL_
 */
public class CheckBound implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.CHECK_BOUND.build(
                any().then(plan -> {
                    checkBound(plan);
                    return null;
                })
            )
        );
    }

    private void checkBound(Plan plan) {
        Set<Unbound> unbounds = plan.getExpressions().stream()
                .<Set<Unbound>>map(e -> e.collect(Unbound.class::isInstance))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        if (!unbounds.isEmpty()) {
            throw new AnalysisException(String.format("unbounded object %s in %s clause.",
                StringUtils.join(unbounds.stream()
                    .map(unbound -> {
                        if (unbound instanceof UnboundSlot) {
                            return ((UnboundSlot) unbound).toSql();
                        } else if (unbound instanceof UnboundFunction) {
                            return ((UnboundFunction) unbound).toSql();
                        }
                        return unbound.toString();
                    })
                    .collect(Collectors.toSet()), ", "),
                    plan.getType().toString().substring("LOGICAL_".length())
            ));
        }
    }
}
