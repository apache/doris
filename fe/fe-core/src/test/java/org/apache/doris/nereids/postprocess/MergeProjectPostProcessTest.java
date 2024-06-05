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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.MergeProjectPostProcessor;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MergeProjectPostProcessTest {
    /**
     * proj1(y, z)
     *    |
     * proj2(x as y, col2 as z, col3)
     *    |
     * proj3(col1 as x, col2, col3)
     *    |
     * SCAN(col1, col2, col3)
     *
     * transform to
     *
     * proj4(col1 as y, col2 as z)
     *    |
     * SCAN(col1, col2, col3)
     *
     */
    @Test
    public void testMergeProj(@Injectable LogicalProperties placeHolder, @Injectable CascadesContext ctx) {
        OlapTable t1 = PlanConstructor.newOlapTable(0, "t1", 0, KeysType.AGG_KEYS);
        List<String> qualifier = new ArrayList<>();
        qualifier.add("test");
        List<Slot> t1Output = new ArrayList<>();
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        t1Output.add(a);
        t1Output.add(b);
        t1Output.add(c);
        LogicalProperties t1Properties = new LogicalProperties(() -> t1Output, () -> DataTrait.EMPTY_TRAIT);
        PhysicalOlapScan scan = new PhysicalOlapScan(RelationId.createGenerator().getNextId(), t1, qualifier, 0L,
                Collections.emptyList(), Collections.emptyList(), null, PreAggStatus.on(), ImmutableList.of(),
                Optional.empty(), t1Properties, Optional.empty());
        Alias x = new Alias(a, "x");
        List<NamedExpression> projList3 = Lists.newArrayList(x, b, c);
        PhysicalProject proj3 = new PhysicalProject(projList3, placeHolder, scan);
        Alias y = new Alias(x.toSlot(), "y");
        Alias z = new Alias(b, "z");
        List<NamedExpression> projList2 = Lists.newArrayList(y, z, c);
        PhysicalProject proj2 = new PhysicalProject(projList2, placeHolder, proj3);
        List<NamedExpression> projList1 = Lists.newArrayList(y.toSlot(), z.toSlot());
        PhysicalProject proj1 = new PhysicalProject(projList1, placeHolder, proj2);
        MergeProjectPostProcessor processor = new MergeProjectPostProcessor();
        PhysicalPlan newPlan = (PhysicalPlan) proj1.accept(processor, ctx);
        Assertions.assertTrue(newPlan instanceof PhysicalProject);
        Assertions.assertTrue(newPlan.child(0) instanceof PhysicalOlapScan);
        List<NamedExpression> resProjList = ((PhysicalProject<?>) newPlan).getProjects();
        Assertions.assertEquals(resProjList.size(), 2);
        Assertions.assertTrue(resProjList.get(0) instanceof Alias);
        Assertions.assertEquals(resProjList.get(0).getName(), "y");
        Assertions.assertEquals(((Alias) resProjList.get(0)).child(), a);
        Assertions.assertTrue(resProjList.get(1) instanceof Alias);
        Assertions.assertEquals(resProjList.get(1).getName(), "z");
        Assertions.assertEquals(((Alias) resProjList.get(1)).child(), b);
    }

}
