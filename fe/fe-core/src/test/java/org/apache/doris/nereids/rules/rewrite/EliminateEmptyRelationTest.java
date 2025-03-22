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

import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link EliminateEmptyRelation}.
 */
class EliminateEmptyRelationTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testEliminateUnionEmptyChild() {
        List<SlotReference> emptyOutput = new ArrayList<>();
        emptyOutput.add(new SlotReference("k", IntegerType.INSTANCE));
        emptyOutput.add(new SlotReference("v", StringType.INSTANCE));
        LogicalEmptyRelation emptyRelation = new LogicalEmptyRelation(new RelationId(1000), emptyOutput);

        List<Plan> children = new ArrayList<>();
        children.add(scan1);
        children.add(emptyRelation);

        List<Slot> scan1Output = scan1.getOutput();

        List<NamedExpression> unionOutput = new ArrayList<>();
        unionOutput.add(scan1Output.get(1));
        unionOutput.add(scan1Output.get(0));

        List<List<SlotReference>> regularOutput = new ArrayList<>();
        regularOutput.add(Lists.newArrayList((SlotReference) scan1Output.get(1), (SlotReference) scan1Output.get(0)));
        regularOutput.add(Lists.newArrayList(emptyOutput.get(1), emptyOutput.get(0)));

        List<List<NamedExpression>> constantExprsList = new ArrayList<>();
        LogicalPlan union = new LogicalUnion(Qualifier.ALL, unionOutput, regularOutput, constantExprsList,
                false, children);

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), union)
                .applyTopDown(new EliminateEmptyRelation());
        Plan plan = checker.getPlan();
        System.out.println(plan.treeString());
        /*
         * LogicalProject[16] ( distinct=false, projects=[name#10003 AS `name`#10003, id#10002 AS `id`#10002] )
         * +--LogicalOlapScan ( qualified=db.t1, indexName=<index_not_selected>, selectedIndexId=-1, preAgg=UNSET, operativeCol=[] )
         */
        //make sure project matches output column to the correct child output column
        checker.matches(logicalProject().when(project -> {
            NamedExpression name = project.getProjects().get(0);
            Assertions.assertEquals("name", name.getName());
            Assertions.assertInstanceOf(SlotReference.class, name.child(0));
            Assertions.assertEquals("name", name.child(0).getExpressionName());
            return true;
        }));
    }
}
