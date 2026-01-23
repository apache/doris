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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PushDownUnnestInProject}.
 */
public class PushDownUnnestInProjectTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    public void testPushDownSingleUnnest() {
        SlotReference arr = new SlotReference("arr", ArrayType.of(IntegerType.INSTANCE));
        Unnest unnest = new Unnest(arr);
        Alias alias = new Alias(unnest, "a");
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(Lists.newArrayList(alias), scan);

        PlanChecker.from(new ConnectContext(), project)
                .applyTopDown(new PushDownUnnestInProject())
                .matchesFromRoot(
                        logicalProject(
                                logicalGenerate(
                                        logicalOlapScan()
                                )
                        ).when(p -> p.getProjects().stream().noneMatch(ne -> ne.containsType(Unnest.class)))
                );
    }

    @Test
    public void testPushDownMultipleUnnest() {
        SlotReference arr1 = new SlotReference("arr1", ArrayType.of(IntegerType.INSTANCE));
        SlotReference arr2 = new SlotReference("arr2", ArrayType.of(IntegerType.INSTANCE));
        Unnest unnest1 = new Unnest(arr1);
        Unnest unnest2 = new Unnest(arr2);
        Alias alias1 = new Alias(unnest1, "a1");
        Alias alias2 = new Alias(unnest2, "a2");
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(Lists.newArrayList(alias1, alias2), scan);

        PlanChecker.from(new ConnectContext(), project)
                .applyTopDown(new PushDownUnnestInProject())
                .matchesFromRoot(
                        logicalProject(
                                logicalGenerate(
                                        logicalOlapScan()
                                )
                        ).when(p -> p.getProjects().stream().allMatch(ne -> ne.child(0) instanceof StructElement))
                );
    }
}
