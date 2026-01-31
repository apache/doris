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

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class CheckUniqueExprIdTest {

    private final LogicalOlapScan student;
    private final SlotReference studentId;
    private final ConnectContext connectContext;

    public CheckUniqueExprIdTest() throws IOException {
        student = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of(""));
        studentId = (SlotReference) student.getOutput().get(0);
        connectContext = TestWithFeService.createDefaultCtx();
        connectContext.getSessionVariable().feDebug = true;
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testNoException() {
        List<NamedExpression> projections = ImmutableList.of(
                new Alias(new Add(studentId, new Random()), "a1"),
                new Alias(new Add(studentId, new Random()), "a2")
        );

        LogicalProject<?> project = new LogicalProject<Plan>(projections, student);
        ExceptionChecker.expectThrowsNoException(() -> new CheckUniqueExprId().rewriteRoot(project, null));
    }

    @Test
    public void testDuplicateSlot() {
        List<NamedExpression> projections = ImmutableList.of(
                new Alias(new Add(studentId, new Random()), "a1"),
                new Alias(new Add(studentId, new Random()), "a2"),
                new Alias(studentId.getExprId(), studentId)
        );

        LogicalProject<?> project = new LogicalProject<Plan>(projections, student);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Found duplicated expr id in two name expression",
                () -> new CheckUniqueExprId().rewriteRoot(project, null));
    }

    @Test
    public void testDuplicateUniqueFunction() {
        Random random = new Random();
        List<NamedExpression> projections = ImmutableList.of(
                new Alias(new Add(studentId, random), "a1"),
                new Alias(new Add(studentId, random), "a2")
        );

        LogicalProject<?> project = new LogicalProject<Plan>(projections, student);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Found duplicated unique id in two unique functions",
                () -> new CheckUniqueExprId().rewriteRoot(project, null));
    }
}
