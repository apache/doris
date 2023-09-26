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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogicalWindowToPhysicalWindowTest implements MemoPatternMatchSupported {

    private Plan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
            ImmutableList.of(""));
    }

    /**
     * select rank() over(partition by id, gender), sum(age) over(partition by gender, id, gender) from student
     *
     * rank and sum have same WindowFrame and compatible partitionKeys, so they can be put to one PhysicalWindow
     */
    @Test
    public void testPartitionKeyCompatible() {
        NamedExpression id = rStudent.getOutput().get(0).toSlot();
        NamedExpression gender = rStudent.getOutput().get(1).toSlot();
        NamedExpression age = rStudent.getOutput().get(3).toSlot();

        List<Expression> pkList1 = ImmutableList.of(id, gender);
        List<Expression> pkList2 = ImmutableList.of(gender, id, gender);
        WindowFrame windowFrame = new WindowFrame(WindowFrame.FrameUnitsType.RANGE,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        WindowExpression window1 = new WindowExpression(new Rank(), pkList1, ImmutableList.of(), windowFrame);
        WindowExpression window2 = new WindowExpression(new Sum(age), pkList2, ImmutableList.of(), windowFrame);
        Alias windowAlias1 = new Alias(window1, window1.toSql());
        Alias windowAlias2 = new Alias(window2, window2.toSql());

        List<NamedExpression> expressions = Lists.newArrayList(windowAlias1, windowAlias2);
        Plan root = new LogicalWindow<>(expressions, rStudent).withChecked(expressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyImplementation(new LogicalWindowToPhysicalWindow().build())
                .matches(
                        physicalWindow()
                        .when(physicalWindow -> {
                            WindowFrameGroup wfg = physicalWindow.getWindowFrameGroup();
                            List<NamedExpression> windows = wfg.getGroups();
                            return windows.get(0).equals(windowAlias1) && windows.get(1).equals(windowAlias2)
                                    && CollectionUtils.isEqualCollection(wfg.getPartitionKeys(), pkList1)
                                    && wfg.getWindowFrame().equals(windowFrame);
                        })
                );
    }
}
