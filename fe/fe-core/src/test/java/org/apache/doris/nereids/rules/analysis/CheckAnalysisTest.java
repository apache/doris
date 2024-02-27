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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckAnalysisTest {
    @Mocked
    private CascadesContext cascadesContext;
    @Mocked
    private GroupPlan groupPlan;

    @Test
    public void testCheckExpressionInputTypes() {
        Plan plan = new LogicalFilter<>(ImmutableSet.of(new And(new IntegerLiteral(1), BooleanLiteral.TRUE)), groupPlan);
        CheckAnalysis checkAnalysis = new CheckAnalysis();
        Assertions.assertThrows(RuntimeException.class, () ->
                checkAnalysis.buildRules().forEach(rule -> rule.transform(plan, cascadesContext)));
    }

    @Test
    public void testCheckNotWithChildrenWithErrorType() {
        Plan plan = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(new Alias(new Not(new IntegerLiteral(2)), "not_2")));
        CheckAnalysis checkAnalysis = new CheckAnalysis();
        Assertions.assertThrows(AnalysisException.class, () ->
                checkAnalysis.buildRules().forEach(rule -> rule.transform(plan, cascadesContext)));
    }
}
