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
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MergeLimitsTest {
    @Test
    public void testMergeConsecutiveLimits() {
        LogicalLimit limit3 = new LogicalLimit<>(3, 5, new UnboundRelation(Lists.newArrayList("db", "t")));
        LogicalLimit limit2 = new LogicalLimit<>(2, 0, limit3);
        LogicalLimit limit1 = new LogicalLimit<>(10, 0, limit2);

        CascadesContext context = MemoTestUtils.createCascadesContext(limit1);
        List<Rule> rules = Lists.newArrayList(new MergeLimits().build());
        context.topDownRewrite(rules);
        LogicalLimit limit = (LogicalLimit) context.getMemo().copyOut();

        Assertions.assertEquals(2, limit.getLimit());
        Assertions.assertEquals(5, limit.getOffset());
        Assertions.assertEquals(1, limit.children().size());
        Assertions.assertTrue(limit.child(0) instanceof UnboundRelation);

    }
}
