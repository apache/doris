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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class BindSinkGroupCommitTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("group_commit_binding");
        connectContext.setDatabase("group_commit_binding");
        createTable("CREATE TABLE generated_middle (k INT, g INT AS (k + 1), v INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE generated_time (k INT, "
                + "producer_time DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP(6), "
                + "g DATETIMEV2(6) AS (now(6))) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
    }

    @Test
    public void consumerUsesGeneratedColumnFromProducer() throws Exception {
        Plan plan = analyzeConsumer("generated_middle");
        assertGeneratedColumnInput(plan, "c2");
        Assertions.assertFalse(plan.anyMatch(p -> ((Plan) p).getExpressions().stream()
                .anyMatch(e -> e.anyMatch(Add.class::isInstance))), plan.treeString());
    }

    @Test
    public void consumerDoesNotEvaluateGeneratedTimeAgain() throws Exception {
        // Referencing c3 proves that the producer's value is consumed, even if now() could be folded.
        assertGeneratedColumnInput(analyzeConsumer("generated_time"), "c3");
    }

    @Test
    public void producerStillComputesGeneratedColumns() {
        boolean previousGroupCommit = connectContext.isGroupCommit();
        connectContext.setGroupCommit(true);
        try {
            Plan plan = analyzeInsert("INSERT INTO generated_middle(k, v) SELECT 1, 99");
            Assertions.assertTrue(plan.anyMatch(p -> ((Plan) p).getExpressions().stream()
                    .anyMatch(e -> e.anyMatch(Add.class::isInstance))), plan.treeString());
        } finally {
            connectContext.setGroupCommit(previousGroupCommit);
        }
    }

    private Plan analyzeConsumer(String tableName) throws Exception {
        long tableId = Env.getCurrentInternalCatalog().getDbOrDdlException("group_commit_binding")
                .getTableOrDdlException(tableName).getId();
        return analyzeInsert("INSERT INTO doris_internal_table_id(" + tableId + ") "
                + "SELECT * FROM group_commit('table_id' = '" + tableId + "')");
    }

    private Plan analyzeInsert(String sql) {
        InsertIntoTableCommand command = (InsertIntoTableCommand) new NereidsParser().parseSingle(sql);
        return PlanChecker.from(connectContext).analyze(command.getLogicalQuery()).getPlan();
    }

    private void assertGeneratedColumnInput(Plan plan, String sourceColumn) {
        List<NamedExpression> generated = plan.collectToList(LogicalProject.class::isInstance).stream()
                .map(p -> (LogicalProject<?>) p)
                .flatMap(p -> p.getProjects().stream())
                .filter(e -> e instanceof Alias && e.getName().equals("g"))
                .collect(Collectors.toList());
        Assertions.assertEquals(1, generated.size(), plan.treeString());
        Assertions.assertEquals(List.of(sourceColumn), generated.get(0).getInputSlots().stream()
                .map(Slot::getName).collect(Collectors.toList()), plan.treeString());
    }
}
