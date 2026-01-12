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

import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

class PushDownFilterThroughWindowTest extends TestWithFeService implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
            PlanConstructor.student,
            ImmutableList.of(""));

    @Test
    void pushDownFilterThroughWindowTest() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        NamedExpression age = scan.getOutput().get(3).toSlot();
        List<Expression> partitionKeyList = ImmutableList.of(age);
        WindowFrame windowFrame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        WindowExpression window1 = new WindowExpression(new RowNumber(), partitionKeyList,
                Lists.newArrayList(), windowFrame);
        Alias windowAlias1 = new Alias(window1, window1.toSql());
        List<NamedExpression> expressions = Lists.newArrayList(windowAlias1);
        LogicalWindow<LogicalOlapScan> window = new LogicalWindow<>(expressions, scan);
        Expression filterPredicate = new EqualTo(age, Literal.of(100));

        LogicalPlan plan = new LogicalPlanBuilder(window)
                .filter(filterPredicate)
                .project(ImmutableList.of(0))
                .build();
        PlanChecker.from(context, plan)
                .applyTopDown(new PushDownFilterThroughWindow())
                .matches(
                        logicalProject(
                                logicalWindow(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> {
                                            if (filter.getConjuncts().size() != 1) {
                                                return false;
                                            }
                                            Expression conj = filter.getConjuncts().iterator().next();
                                            if (!(conj instanceof EqualTo)) {
                                                return false;
                                            }
                                            EqualTo eq = (EqualTo) conj;
                                            return eq.left().equals(age);

                                        })
                                )
                        )
                );
    }

    @Test
    public void testPushDownFilter() throws Exception {
        String db = "test";
        createDatabase(db);
        useDatabase(db);
        createTable("CREATE TABLE lineorders (\n"
                + "orderdate varchar(100) NOT NULL,\n"
                + "orderid int NOT NULL,\n"
                + "country_id int NOT NULL,\n"
                + "vender_id int NOT NULL,\n"
                + "ordernum int NOT NULL,\n"
                + "ordemoney int NOT NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(orderdate, orderid, country_id)\n"
                + "COMMENT 'OLAP'\n"
                + "PARTITION BY LIST(orderdate)\n"
                + "(PARTITION p1992 VALUES IN (\"0-2020\"),\n"
                + "PARTITION p1993 VALUES IN (\"0-2021\"),\n"
                + "PARTITION p1994 VALUES IN (\"0-2022\"),\n"
                + "PARTITION p1995 VALUES IN (\"0-2023\"),\n"
                + "PARTITION p1996 VALUES IN (\"0-2024\"),\n"
                + "PARTITION p1997 VALUES IN (\"0-2025\"))\n"
                + "DISTRIBUTED BY HASH(orderid) BUCKETS 48\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ")");

        connectContext.getSessionVariable()
                .setDisableNereidsRules(
                        RuleType.OLAP_SCAN_PARTITION_PRUNE.name() + "," + RuleType.PRUNE_EMPTY_PARTITION.name());

        PlanChecker.from(connectContext)
                .analyze("select * from ( \n"
                        + "  select \n"
                        + "    orderid,\n"
                        + "    orderdate,\n"
                        + "    country_id,\n"
                        + "    ordernum,\n"
                        + "    ordemoney,\n"
                        + "    SUBSTR(lineorders.orderdate,3,4) AS dt,\n"
                        + "    ROW_NUMBER() OVER(PARTITION BY lineorders.orderid,lineorders.orderdate ORDER BY lineorders.country_id DESC) AS rn\n"
                        + "  from lineorders\n"
                        + ") a \n"
                        + "where SUBSTR(a.dt, 1, 4) = SUBSTR(curdate(), 1, 4)")
                .rewrite()
                .matchesFromRoot(
                        logicalResultSink(
                                logicalProject(
                                        logicalWindow(
                                                logicalProject(
                                                        logicalFilter(
                                                                logicalOlapScan()
                                                        )
                                                )
                                        )
                                )
                        )
                );
    }
}
