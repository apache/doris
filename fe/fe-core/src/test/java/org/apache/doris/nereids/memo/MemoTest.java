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

package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class MemoTest {
    @Test
    public void testCopyIn() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject insideProject = new LogicalProject(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject rootProject = new LogicalProject(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);

        Group rootGroup = memo.getRoot();

        Assert.assertEquals(3, memo.getGroups().size());
        Assert.assertEquals(3, memo.getGroupExpressions().size());

        Assert.assertEquals(PlanType.LOGICAL_PROJECT, rootGroup.logicalExpressionsAt(0).getPlan().getType());
        Assert.assertEquals(PlanType.LOGICAL_PROJECT,
                rootGroup.logicalExpressionsAt(0).child(0).logicalExpressionsAt(0).getPlan().getType());
        Assert.assertEquals(PlanType.LOGICAL_UNBOUND_RELATION,
                rootGroup.logicalExpressionsAt(0).child(0).logicalExpressionsAt(0).child(0).logicalExpressionsAt(0)
                        .getPlan().getType());
    }
}
