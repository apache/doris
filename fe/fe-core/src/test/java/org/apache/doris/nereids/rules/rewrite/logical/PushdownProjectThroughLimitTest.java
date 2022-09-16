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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PushdownProjectThroughLimitTest {

    @Mocked
    private CascadesContext cascadesContext;

    @Test
    public void testPushdownProjectThroughLimit(@Mocked GroupPlan groupPlan) {
        SlotReference slotRef = new SlotReference("col1", IntegerType.INSTANCE);
        LogicalLimit logicalLimit = new LogicalLimit(1, 1, groupPlan);
        LogicalProject logicalProject = new LogicalProject(ImmutableList.of(slotRef), logicalLimit);
        PushdownProjectThroughLimit pushdownProjectThroughLimit = new PushdownProjectThroughLimit();
        LogicalPlan rewrittenPlan =
                (LogicalPlan) pushdownProjectThroughLimit.build().transform(logicalProject, cascadesContext).get(0);
        Assertions.assertTrue(rewrittenPlan instanceof LogicalLimit);
        Assertions.assertTrue(rewrittenPlan.child(0) instanceof LogicalProject);
    }
}
