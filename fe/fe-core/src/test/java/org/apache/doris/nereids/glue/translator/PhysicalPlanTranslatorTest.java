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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PhysicalPlanTranslatorTest {

    @Test
    public void testOlapPrune(@Injectable LogicalProperties placeHolder) throws Exception {
        OlapTable t1 = PlanConstructor.newOlapTable(0, "t1", 0, KeysType.AGG_KEYS);
        List<String> qualifier = new ArrayList<>();
        qualifier.add("test");
        List<Slot> t1Output = new ArrayList<>();
        SlotReference col1 = new SlotReference("col1", IntegerType.INSTANCE);
        SlotReference col2 = new SlotReference("col2", IntegerType.INSTANCE);
        SlotReference col3 = new SlotReference("col2", IntegerType.INSTANCE);
        t1Output.add(col1);
        t1Output.add(col2);
        t1Output.add(col3);
        LogicalProperties t1Properties = new LogicalProperties(() -> t1Output);
        PhysicalOlapScan scan = new PhysicalOlapScan(StatementScopeIdGenerator.newRelationId(), t1, qualifier, t1.getBaseIndexId(),
                Collections.emptyList(), Collections.emptyList(), null, PreAggStatus.on(),
                ImmutableList.of(), Optional.empty(), t1Properties, Optional.empty());
        Literal t1FilterRight = new IntegerLiteral(1);
        Expression t1FilterExpr = new GreaterThan(col1, t1FilterRight);
        PhysicalFilter<PhysicalOlapScan> filter =
                new PhysicalFilter<>(ImmutableSet.of(t1FilterExpr), placeHolder, scan);
        List<NamedExpression> projList = new ArrayList<>();
        projList.add(col2);
        PhysicalProject<PhysicalFilter<PhysicalOlapScan>> project = new PhysicalProject<>(projList,
                placeHolder, filter);
        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(planTranslatorContext, null);
        PlanFragment fragment = translator.visitPhysicalProject(project, planTranslatorContext);
        PlanNode planNode = fragment.getPlanRoot();
        List<OlapScanNode> scanNodeList = new ArrayList<>();
        planNode.collect(OlapScanNode.class::isInstance, scanNodeList);
        Assertions.assertEquals(2, scanNodeList.get(0).getTupleDesc().getMaterializedSlots().size());
    }
}
