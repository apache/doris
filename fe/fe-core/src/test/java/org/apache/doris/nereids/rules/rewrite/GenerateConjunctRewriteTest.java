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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests that ExprId replacement (e.g. any_value wrapping in EliminateGroupByKeyByUniform /
 * EliminateGroupByKey) is also applied to LogicalGenerate lateral ON conjuncts.
 */
class GenerateConjunctRewriteTest extends TestWithFeService {

    @Test
    void testExprIdRewriterRewritesLateralConjuncts() {
        // Review P1: GenerateExpressionRewrite rewrote only getGenerators(); lateral ON
        // conjuncts kept the stale ExprId after the child switched to wrapped slots, so
        // final slot validation rejected the query. The conjuncts must be rewritten
        // together with the generators.
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference x = new SlotReference("x", VarcharType.SYSTEM_DEFAULT);
        Unnest generator = new Unnest(new IntegerLiteral(1));
        EqualTo conjunct = new EqualTo(k, a);
        LogicalGenerate<Plan> generate = new LogicalGenerate<>(
                ImmutableList.of(generator), ImmutableList.of(x), ImmutableList.of(),
                ImmutableList.of(conjunct), new LogicalOneRowRelation(
                        StatementScopeIdGenerator.newRelationId(), ImmutableList.of(a)));

        ExprId newK = new ExprId(999);
        Map<ExprId, ExprId> replaceMap = ImmutableMap.of(k.getExprId(), newK);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, generate);
        ExprIdRewriter rewriter = new ExprIdRewriter(new ExprIdRewriter.ReplaceRule(replaceMap, true),
                new JobContext(cascadesContext, PhysicalProperties.ANY));
        LogicalGenerate<?> newGenerate = (LogicalGenerate<?>) rewriter.rewriteExpr(generate, replaceMap);

        Assertions.assertEquals(1, newGenerate.getConjuncts().size());
        Slot newKSlot = (Slot) ((EqualTo) newGenerate.getConjuncts().get(0)).child(0);
        Assertions.assertEquals(newK, newKSlot.getExprId(),
                "lateral ON conjunct must be rewritten with the new ExprId");
    }
}
