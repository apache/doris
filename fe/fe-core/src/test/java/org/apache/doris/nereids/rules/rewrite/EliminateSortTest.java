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

import org.apache.doris.catalog.Database;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * column prune ut.
 */
class EliminateSortTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int,sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
    }

    @Test
    void testEliminateSortWhenCTE() {
        PlanChecker.from(connectContext)
                .analyze("with cte_test as (\n"
                        + "select id, name, age from student order by id\n"
                        + ")\n"
                        + "select * from cte_test c1\n"
                        + "join student\n"
                        + "join cte_test c2\n"
                        + "where c1.id =  student.id")
                .rewrite()
                .nonMatch(logicalSort());

        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("with cte_test as (\n"
                        + "select id, name, age from student\n"
                        + ")\n"
                        + "select * from cte_test c1\n"
                        + "join student\n"
                        + "join cte_test c2\n"
                        + "where c1.id =  student.id order by c1.age")
                .rewrite()
                .matches(logicalSort());

        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("select t.age from\n"
                        + "(\n"
                        + "with cte_test as (\n"
                        + "select id, name, age from student order by id\n"
                        + ")\n"
                        + "select c1.id, student.name, c2.age from cte_test c1\n"
                        + "join student\n"
                        + "join cte_test c2\n"
                        + "where c1.id =  student.id order by c1.age\n"
                        + ") t order by t.id")
                .rewrite()
                .matches(logicalSort());
    }

    @Test
    void testEliminateSortUnderTableSink() {
        // topN under table sink should not be removed
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .sort(scan.getOutput().stream().map(c -> new OrderKey(c, true, true)).collect(Collectors.toList()))
                .limit(1, 1).build();
        plan = new LogicalOlapTableSink<>(new Database(), scan.getTable(), scan.getTable().getBaseSchema(),
                new ArrayList<>(), plan.getOutput().stream().map(NamedExpression.class::cast).collect(
                Collectors.toList()), false, DMLCommandType.NONE, plan);
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .rewrite()
                .nonMatch(logicalSort())
                .matches(logicalTopN());

        // sort under table sink should be removed
        scan = PlanConstructor.newLogicalOlapScan(0, "t2", 0);
        plan = new LogicalPlanBuilder(scan)
                .sort(scan.getOutput().stream().map(c -> new OrderKey(c, true, true)).collect(Collectors.toList()))
                .build();
        plan = new LogicalOlapTableSink<>(new Database(), scan.getTable(), scan.getTable().getBaseSchema(),
                new ArrayList<>(), plan.getOutput().stream().map(NamedExpression.class::cast).collect(
                Collectors.toList()), false, DMLCommandType.NONE, plan);
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .rewrite()
                .nonMatch(logicalSort());
    }

    @Test
    void testEliminateSortInUnion() {
        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("SELECT * FROM (SELECT * FROM student UNION SELECT * FROM student ORDER BY id) u  LIMIT 1")
                .rewrite()
                .nonMatch(logicalSort());
    }

    @Test
    void testEliminateSortInSubquery() {
        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("select count(*) from (select * from student order by id) t")
                .rewrite()
                .nonMatch(logicalSort());
        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("select \n"
                        + "  id, \n"
                        + "  name \n"
                        + "from \n"
                        + "  (\n"
                        + "    select \n"
                        + "      id, \n"
                        + "      name, \n"
                        + "      age \n"
                        + "    from \n"
                        + "      (\n"
                        + "        select \n"
                        + "          * \n"
                        + "        from \n"
                        + "          student\n"
                        + "      ) s \n"
                        + "    order by \n"
                        + "      id\n"
                        + "  ) t")
                .rewrite()
                .nonMatch(logicalSort());
    }

    @Test
    void testSortLimit() {
        PlanChecker.from(connectContext).disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("select count(*) from (select * from student order by id) t limit 1")
                .rewrite()
                // there is no topn below agg
                .matches(logicalTopN(logicalAggregate(logicalProject(logicalOlapScan()))));
        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("select count(*) from (select * from student order by id limit 1) t")
                .rewrite()
                .matches(logicalTopN());

        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("select count(*) from "
                        + "(select * from student order by id limit 1) t1 left join student t2 on t1.id = t2.id")
                .rewrite()
                .matches(logicalTopN());
        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze("select count(*) from "
                        + "(select * from student order by id) t1 left join student t2 on t1.id = t2.id limit 1")
                .rewrite()
                .matches(logicalTopN(logicalAggregate(logicalProject(logicalJoin(
                        logicalProject(logicalOlapScan()),
                        logicalProject(logicalOlapScan()))))));
    }
}
