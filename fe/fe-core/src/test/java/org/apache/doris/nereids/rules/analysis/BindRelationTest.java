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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.analysis.BindRelation.CustomTableResolver;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

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

        Assertions.assertTrue(plan instanceof LogicalOlapScan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "t"),
                ((LogicalOlapScan) plan).qualified());
    }

    @Test
    void bindByDbQualifier() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB2);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("db1", "t")),
                connectContext, new BindRelation());

        Assertions.assertTrue(plan instanceof LogicalOlapScan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "t"),
                ((LogicalOlapScan) plan).qualified());
    }

    @Test
    public void bindExternalRelation() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB1);
        String tableName = "external_table";

        List<Column> externalTableColumns = ImmutableList.of(
                new Column("id", Type.INT),
                new Column("name", Type.VARCHAR)
        );

        OlapTable externalOlapTable = new OlapTable(1, tableName, externalTableColumns, KeysType.DUP_KEYS,
                new PartitionInfo(), new RandomDistributionInfo(10)) {
            @Override
            public List<Column> getBaseSchema(boolean full) {
                return externalTableColumns;
            }

            @Override
            public boolean hasDeleteSign() {
                return false;
            }
        };

        CustomTableResolver customTableResolver = qualifiedTable -> {
            if (qualifiedTable.get(2).equals(tableName)) {
                return externalOlapTable;
            } else {
                return null;
            }
        };

        PlanChecker.from(connectContext)
                .parse("select * from " + tableName + " as et join db1.t on et.id = t.a")
                .customAnalyzer(Optional.of(customTableResolver)) // analyze internal relation
                .matches(
                        logicalJoin(
                                logicalSubQueryAlias(
                                    logicalOlapScan().when(r -> r.getTable() == externalOlapTable)
                                ),
                                logicalOlapScan().when(r -> r.getTable().getName().equals("t"))
                        )
                );
    }

    @Test
    void bindRandomAggTable() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB1);
        connectContext.getState().setIsQuery(true);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("tagg")),
                connectContext, new BindRelation());

        Assertions.assertTrue(plan instanceof LogicalAggregate);
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
