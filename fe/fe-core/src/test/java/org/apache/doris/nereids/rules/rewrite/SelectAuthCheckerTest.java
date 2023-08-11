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

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class SelectAuthCheckerTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int,sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        connectContext.setDatabase("default_cluster:test");
    }

    @Test
    public void testPruneColumns20() {
        Set<String> respect = Sets.newHashSet("id", "age", "name");

        Plan plan = PlanChecker.from(connectContext).analyze("select id,name from student where age = 10").getPlan();
        Set<String> result = getFilterCols((LogicalProject<LogicalFilter<LogicalRelation>>) plan.child(0));
        Assert.assertTrue(CollectionUtils.isEqualCollection(result, respect));

        plan = PlanChecker.from(connectContext).analyze("select id+1,name from student where age = 10").getPlan();
        result = getFilterCols((LogicalProject<LogicalFilter<LogicalRelation>>) plan.child(0));
        Assert.assertTrue(CollectionUtils.isEqualCollection(result, respect));

        plan = PlanChecker.from(connectContext).analyze("select id,name from student where age+1 = 10").getPlan();
        result = getFilterCols((LogicalProject<LogicalFilter<LogicalRelation>>) plan.child(0));
        Assert.assertTrue(CollectionUtils.isEqualCollection(result, respect));

        plan = PlanChecker.from(connectContext).analyze("select id,name,age from student").getPlan();
        result = getRelationCols((LogicalProject<LogicalRelation>) plan.child(0));
        Assert.assertTrue(CollectionUtils.isEqualCollection(result, respect));
    }

    private Set<String> getFilterCols(LogicalProject<LogicalFilter<LogicalRelation>> project) {
        Plan plan = project.child(0);
        LogicalFilter filter = (LogicalFilter) plan;
        Set<String> cols = Sets.newHashSet();
        // get cols from filter
        SelectAuthChecker.getCols(filter.getConjuncts(), cols);
        // get cols from project
        SelectAuthChecker.getCols(project.getProjects(), cols);
        return cols;
    }

    private Set<String> getRelationCols(LogicalProject<LogicalRelation> project) {
        Set<String> cols = Sets.newHashSet();
        // get cols from project
        SelectAuthChecker.getCols(project.getProjects(), cols);
        return cols;
    }
}
