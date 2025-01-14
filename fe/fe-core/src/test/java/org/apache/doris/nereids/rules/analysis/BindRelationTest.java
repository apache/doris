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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BindRelationTest extends TestWithFeService implements GeneratedPlanPatterns {
    private static final String DB1 = "db1";
    private static final String DB2 = "db2";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB1);
        createTable("CREATE TABLE db1.t ( \n"
                + " \ta INT,\n"
                + " \tb VARCHAR\n"
                + ")ENGINE=OLAP\n"
                + "DISTRIBUTED BY HASH(`a`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\"= \"1\");");
        createTable("CREATE TABLE db1.tagg ( \n"
                + " \ta INT,\n"
                + " \tb INT SUM\n"
                + ")ENGINE=OLAP AGGREGATE KEY(a)\n "
                + "DISTRIBUTED BY random BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\"= \"1\");");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void bindInCurrentDb() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB1);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("t")),
                connectContext, new BindRelation());

        Assertions.assertInstanceOf(LogicalOlapScan.class, plan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "t"),
                ((LogicalOlapScan) plan).qualified());
    }

    @Test
    void bindByDbQualifier() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB2);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("db1", "t")),
                connectContext, new BindRelation());

        Assertions.assertInstanceOf(LogicalOlapScan.class, plan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "t"),
                ((LogicalOlapScan) plan).qualified());
    }

    @Test
    void bindRandomAggTable() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB1);
        connectContext.getState().setIsQuery(true);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("tagg")),
                connectContext, new BindRelation());

        Assertions.assertInstanceOf(LogicalAggregate.class, plan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "tagg"),
                plan.getOutput().get(0).getQualifier());
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "tagg"),
                plan.getOutput().get(1).getQualifier());
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }
}
