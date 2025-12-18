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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.TopDownVisitorRewriteJob;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.FilteredRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class SetOperationBucketShuffleTest extends TestWithFeService implements PlanPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.t1(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1')");
        createTable("create table test.t2(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1')");
        createTable("create table test.t3(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1')");
    }

    @Test
    public void testBucketShuffleUnion() throws Exception {
        assertMatches(
                "select * from (select * from test.t1 union all select * from test.t2)a join[shuffle] (select * from test.t1)b on a.id=b.id",
                physicalHashJoin(
                        physicalUnion(
                                logicalOlapScan(),
                                physicalDistribute(
                                        logicalOlapScan()
                                )
                        ),
                        physicalDistribute(
                                physicalOlapScan()
                        )
                )
        );
    }

    private void assertMatches(String sql, PatternDescriptor<?> pattern) throws Exception {
        AtomicBoolean matches = new AtomicBoolean(false);
        FilteredRules filteredRules = new FilteredRules(
                ImmutableList.of(
                        new OneRewriteRuleFactory() {
                            @Override
                            public Rule build() {
                                return pattern.then(x -> {
                                    matches.set(true);
                                    return x;
                                }).toRule(RuleType.TEST_REWRITE);
                            }
                        }.build()
                )
        );

        TopDownVisitorRewriteJob matchesJob = new TopDownVisitorRewriteJob(
                filteredRules,
                (p) -> true
        );

        PhysicalPlan physicalPlan = parseToPhysicalPlan(sql);
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        PhysicalProperties requestProperties = NereidsPlanner.buildInitRequireProperties();
        CascadesContext cascadesContext = CascadesContext.initContext(
                statementContext, physicalPlan, requestProperties);
        matchesJob.execute(new JobContext(cascadesContext, PhysicalProperties.ANY, Double.MAX_VALUE));
        Assertions.assertTrue(matches.get(), () -> "Real shape:\n" + physicalPlan.treeString());
    }

    private PhysicalPlan parseToPhysicalPlan(String sql) throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        StmtExecutor stmtExecutor = executeNereidsSql(sql);
        NereidsPlanner planner = (NereidsPlanner) stmtExecutor.planner();
        return planner.getPhysicalPlan();
    }
}
