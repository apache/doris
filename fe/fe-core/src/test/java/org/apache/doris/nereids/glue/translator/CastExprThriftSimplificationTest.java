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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Verifies that a no-op cast (e.g. `cast(char_col as text)`) is eliminated during
 * thrift serialization even when it is a nested child of another expression
 * (e.g. `cast(a as text) = '1'` should be serialized as `a = '1'`).
 */
public class CastExprThriftSimplificationTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.t (a char(6), b int) "
                + "distributed by hash(b) buckets 1 "
                + "properties('replication_num' = '1');");
        // Avoid the empty scan being pruned away in the test environment.
        connectContext.getSessionVariable().setDisableNereidsRules("prune_empty_partition");
    }

    @Test
    public void testNoOpCastInConjunctIsEliminatedInThrift() throws Exception {
        String sql = "select * from test.t where cast(a as text) = '1'";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        PhysicalPlan plan = new NereidsPlanner(statementContext).planWithLock(
                new NereidsParser().parseSingle(sql),
                PhysicalProperties.ANY
        );

        PlanFragment fragment = new PhysicalPlanTranslator(new PlanTranslatorContext()).translatePlan(plan);
        PlanNode planNode = fragment.getPlanRoot();

        List<PlanNode> allNodes = new ArrayList<>();
        planNode.foreachDown(n -> {
            allNodes.add((PlanNode) n);
            return true;
        });

        boolean foundCastExpr = false;
        for (PlanNode node : allNodes) {
            for (Expr e : node.getConjuncts()) {
                for (TExprNode tnode : e.treeToThrift().getNodes()) {
                    if (tnode.getNodeType() == TExprNodeType.CAST_EXPR) {
                        foundCastExpr = true;
                    }
                }
            }
        }
        Assertions.assertFalse(foundCastExpr,
                "no-op cast should be eliminated during thrift serialization, but CAST_EXPR node is present");
    }

    @Test
    public void testNonNoOpCastInConjunctIsKeptInThrift() throws Exception {
        String sql = "select * from test.t where cast(b as text) = '1'";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        PhysicalPlan plan = new NereidsPlanner(statementContext).planWithLock(
                new NereidsParser().parseSingle(sql),
                PhysicalProperties.ANY
        );

        PlanFragment fragment = new PhysicalPlanTranslator(new PlanTranslatorContext()).translatePlan(plan);
        PlanNode planNode = fragment.getPlanRoot();

        List<PlanNode> allNodes = new ArrayList<>();
        planNode.foreachDown(n -> {
            allNodes.add((PlanNode) n);
            return true;
        });

        boolean foundCastExpr = false;
        for (PlanNode node : allNodes) {
            for (Expr e : node.getConjuncts()) {
                for (TExprNode tnode : e.treeToThrift().getNodes()) {
                    if (tnode.getNodeType() == TExprNodeType.CAST_EXPR) {
                        foundCastExpr = true;
                    }
                }
            }
        }
        Assertions.assertTrue(foundCastExpr,
                "int -> text cast is a real cast and must be kept during thrift serialization");
    }
}
