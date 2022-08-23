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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NormalizeAggregateTest implements PatternMatchSupported {
    private Plan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(PlanConstructor.student, ImmutableList.of("student"));
    }

    @Test
    public void test() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot(),
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matchesFromRoot(logicalProject(logicalAggregate(logicalOlapScan())));
    }
}
