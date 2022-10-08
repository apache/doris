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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class PlanUtilsTest {

    @Test
    void projectOrSelf() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Plan self = PlanUtils.projectOrSelf(Lists.newArrayList(), scan);
        Assertions.assertSame(scan, self);

        NamedExpression slot = scan.getOutput().get(0);
        List<NamedExpression> projects = Lists.newArrayList();
        projects.add(slot);
        Plan project = PlanUtils.projectOrSelf(projects, scan);
        Assertions.assertTrue(project instanceof LogicalProject);
    }

    @Test
    void filterOrSelf() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Plan filterOrSelf = PlanUtils.filterOrSelf(Lists.newArrayList(), scan);
        Assertions.assertSame(scan, filterOrSelf);

        List<Expression> predicate = Lists.newArrayList();
        predicate.add(BooleanLiteral.TRUE);
        Plan filter = PlanUtils.filterOrSelf(predicate, scan);
        Assertions.assertTrue(filter instanceof LogicalFilter);
    }
}
