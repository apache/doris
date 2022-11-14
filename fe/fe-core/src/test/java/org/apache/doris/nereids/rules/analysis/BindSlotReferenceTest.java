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
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BindSlotReferenceTest {

    @BeforeEach
    public void beforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    public void testCannotFindSlot() {
        LogicalProject project = new LogicalProject<>(ImmutableList.of(new UnboundSlot("foo")),
                new LogicalOlapScan(new RelationId(0), PlanConstructor.student));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(project));
        Assertions.assertEquals("Cannot find column foo.", exception.getMessage());
    }

    @Test
    public void testAmbiguousSlot() {
        LogicalOlapScan scan1 = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalOlapScan scan2 = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = new LogicalJoin<>(
                JoinType.CROSS_JOIN, scan1, scan2);
        LogicalProject<LogicalJoin<LogicalOlapScan, LogicalOlapScan>> project = new LogicalProject<>(
                ImmutableList.of(new UnboundSlot("id")), join);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(project));
        Assertions.assertTrue(exception.getMessage().contains("id is ambiguous: "));
        Assertions.assertTrue(exception.getMessage().contains("id#4"));
        Assertions.assertTrue(exception.getMessage().contains("id#0"));
    }
}
