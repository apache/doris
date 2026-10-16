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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ConstantPropagation materializes a constant output above the aggregate with a fresh exprId.
 *
 * <p>The project item used to reuse the exprId of the aggregate output it replaces. Then one exprId had two
 * definitions, the aggregate output slot and the constant, whose nullable and data type may differ, and rules
 * which compare exprIds only, e.g. EliminateUnnecessaryProject, could treat them as the same column and remove
 * the constant project. RQG case 11000258898 reached such a plan and the last AdjustNullable pass then reported
 * "AdjustNullable convert slot 2004-04-28#xx from not-nullable to nullable".
 */
public class ConstantPropagationFreshExprIdTest extends TestWithFeService {
    private static final String SQL_11000258898 = "select '2004-04-28', '2014-07-07'"
            + " intersect distinct"
            + " select col_datetime_3__undef_signed_not_null, col_date_undef_signed_not_null"
            + " from table_100_undef_partitions2_keys3_properties4_distributed_by52"
            + " where col_decimal_12_2__undef_signed = 94.0212"
            + " union distinct"
            + " select col_datetime_3__undef_signed, col_datetime_3__undef_signed_not_null"
            + " from table_100_undef_partitions2_keys3_properties4_distributed_by54"
            + " where col_datetime_3__undef_signed = '2011-11-15 13:33:05'"
            + " intersect"
            + " select col_date_undef_signed_not_null, col_date_undef_signed_not_null"
            + " from table_100_undef_partitions2_keys3_properties4_distributed_by53"
            + " intersect"
            + " select col_datetime_3__undef_signed_not_null, col_date_undef_signed_not_null"
            + " from table_100_undef_partitions2_keys3_properties4_distributed_by54 ;";

    private static final String SQL_MATERIALIZED_CTE = "WITH c AS (SELECT k, v, count(*) AS cnt"
            + " FROM constant_propagation_fresh_expr_id_t WHERE k = 5 GROUP BY k, v)"
            + " SELECT l.k, l.v, l.cnt, r.k, r.v, r.cnt FROM c l CROSS JOIN c r";

    private static final String SQL_MATERIALIZED_CTE_CONSTANT_COLUMN
            = "WITH c AS (SELECT v, k + 0 AS x, count(*) AS cnt"
            + " FROM constant_propagation_fresh_expr_id_t WHERE k = 5 GROUP BY v, k)"
            + " SELECT l.v, l.x, l.cnt, r.v, r.x, r.cnt FROM c l CROSS JOIN c r";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.constant_propagation_fresh_expr_id_t(k int null, v int null)"
                + " distributed by hash(k) properties('replication_num' = '1');");
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by52 ("
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_date_undef_signed_not_null date not null,"
                + " col_date_undef_signed date null,"
                + " col_decimal_12_2__undef_signed decimal(12,2) null,"
                + " col_decimal_32_6__undef_signed decimal(32,6) null)"
                + " distributed by hash(col_datetime_3__undef_signed) properties('replication_num' = '1');");
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by53 ("
                + " col_date_undef_signed_not_null date not null,"
                + " col_date_undef_signed date null,"
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_varchar_25__undef_signed varchar(25) null,"
                + " col_char_25__undef_signed_not_null char(25) not null,"
                + " col_char_25__undef_signed char(25) null,"
                + " col_char_40__undef_signed char(40) null,"
                + " col_varchar_100__undef_signed varchar(100) null,"
                + " col_tinyint_undef_signed_not_null tinyint not null,"
                + " col_bigint_undef_signed_not_null bigint not null)"
                + " distributed by hash(col_date_undef_signed_not_null) properties('replication_num' = '1');");
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by54 ("
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_date_undef_signed_not_null date not null,"
                + " col_date_undef_signed date null,"
                + " col_char_25__undef_signed_not_null char(25) not null,"
                + " col_char_40__undef_signed char(40) null)"
                + " distributed by hash(col_datetime_3__undef_signed) properties('replication_num' = '1');");
        createTable("create table test.table_100_undef_partitions2_keys3_properties4_distributed_by5 ("
                + " col_date_undef_signed_not_null date not null,"
                + " col_datetime_undef_signed datetime null,"
                + " col_datetime_undef_signed_not_null datetime not null,"
                + " col_datetime_3__undef_signed datetime(3) null,"
                + " col_datetime_3__undef_signed_not_null datetime(3) not null,"
                + " col_char_25__undef_signed char(25) null,"
                + " col_char_25__undef_signed_not_null char(25) not null,"
                + " col_varchar_100__undef_signed_not_null varchar(100) not null,"
                + " col_varchar_100__undef_signed varchar(100) null)"
                + " distributed by hash(col_date_undef_signed_not_null) properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testConstantAggregateProjectUsesFreshExprId() {
        // run this rule alone, so the project it materializes above the aggregate is still part of the plan
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze("select k, v from constant_propagation_fresh_expr_id_t where k = 5 and v = 7 group by k, v")
                .applyCustom(new ConstantPropagation());
        Plan plan = checker.getPlan();
        LogicalProject<?> constantProject = findConstantProject(plan);
        Assertions.assertNotNull(constantProject, plan.treeString());
        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) constantProject.child();
        Set<ExprId> aggregateOutputIds = aggregate.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toSet());
        Set<ExprId> constantItemIds = constantProject.getProjects().stream()
                .map(NamedExpression::getExprId).collect(Collectors.toSet());
        // the constant items describe other values than the aggregate outputs, so they must not reuse their exprIds
        Assertions.assertTrue(Collections.disjoint(aggregateOutputIds, constantItemIds),
                "the constant project reuses the exprId of the aggregate output:\n" + plan.treeString());
        // and the plan above has to reference the fresh exprIds
        Set<ExprId> rootOutputIds = plan.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toSet());
        Assertions.assertTrue(rootOutputIds.containsAll(constantItemIds),
                "the plan above does not reference the fresh exprIds:\n" + plan.treeString());
    }

    @Test
    void testRqgCase11000258898() {
        // before the fix this query tripped the AdjustNullable check with
        // "AdjustNullable convert slot 2004-04-28#xx from not-nullable to nullable"
        PlanChecker checker = PlanChecker.from(connectContext).analyze(SQL_11000258898).rewrite();
        assertNoShadowProject(checker.getPlan());
    }

    @Test
    void testMaterializedCteConsumerMapsStayValid() throws Exception {
        // the producer body and the consumers are rewritten in different subtree contexts and the consumers are
        // rewritten first, so the fresh exprIds of the producer outputs must not cross the CTE boundary
        Plan plan = PlanChecker.from(connectContext).analyze(SQL_MATERIALIZED_CTE).rewrite().getPlan();
        assertCteConsumerMapsStayValid(plan);
        // the query has to stay plannable
        PlanChecker.from(connectContext).checkPlannerResult(SQL_MATERIALIZED_CTE);
    }

    /** Every producer slot a CTE consumer maps to has to be an output slot of the producer. */
    private static void assertCteConsumerMapsStayValid(Plan plan) {
        Set<ExprId> producerOutputExprIds = new java.util.HashSet<>();
        Set<LogicalCTEConsumer> consumers = new java.util.HashSet<>();
        plan.foreach(node -> {
            if (node instanceof LogicalCTEProducer) {
                ((LogicalCTEProducer<?>) node).getOutput().forEach(slot -> producerOutputExprIds.add(slot.getExprId()));
            } else if (node instanceof LogicalCTEConsumer) {
                consumers.add((LogicalCTEConsumer) node);
            }
            return false;
        });
        Assertions.assertFalse(consumers.isEmpty(), plan.treeString());
        for (LogicalCTEConsumer consumer : consumers) {
            for (Slot producerSlot : consumer.getProducerToConsumerOutputMap().keySet()) {
                Assertions.assertTrue(producerOutputExprIds.contains(producerSlot.getExprId()),
                        "the consumer map of " + consumer.getRelationId() + " references " + producerSlot
                                + " which the producer does not output: " + plan.treeString());
            }
            for (Slot producerSlot : consumer.getConsumerToProducerOutputMap().values()) {
                Assertions.assertTrue(producerOutputExprIds.contains(producerSlot.getExprId()),
                        "the consumer map of " + consumer.getRelationId() + " references " + producerSlot
                                + " which the producer does not output: " + plan.treeString());
            }
        }
    }

    @Test
    void testMaterializedCteKeepsConstantProducerOutput() {
        // `k + 0` is uniform (k = 5), so it folds to a constant, but it is an output of the CTE producer and not
        // a grouping key: the producer has to keep it as it is, its consumers map it by exprId
        Plan plan = PlanChecker.from(connectContext)
                .analyze(SQL_MATERIALIZED_CTE_CONSTANT_COLUMN).rewrite().getPlan();
        assertCteConsumerMapsStayValid(plan);
        Assertions.assertEquals(3, ((LogicalCTEProducer<?>) plan.child(0)).getOutput().size(), plan.treeString());
        PlanChecker.from(connectContext).checkPlannerResult(SQL_MATERIALIZED_CTE_CONSTANT_COLUMN);
    }

    private static LogicalProject<?> findConstantProject(Plan plan) {
        if (plan instanceof LogicalProject
                && plan.child(0) instanceof LogicalAggregate
                && ((LogicalProject<?>) plan).getProjects().stream().anyMatch(item -> !(item instanceof SlotReference))) {
            return (LogicalProject<?>) plan;
        }
        for (Plan child : plan.children()) {
            LogicalProject<?> found = findConstantProject(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * A project may output an exprId which its child already outputs only when the item describes the same
     * value as that child slot: a plain reference of it, or the no-op wrapper {@code Alias(X, slotX)}. Anything
     * else would give one exprId two definitions, and every rule which compares exprIds only would take them as
     * the same column.
     */
    private static void assertNoShadowProject(Plan plan) {
        Set<ExprId> childrenExprIds = plan.children().stream()
                .flatMap(child -> child.getOutput().stream())
                .map(Slot::getExprId)
                .collect(Collectors.toSet());
        if (plan instanceof LogicalProject) {
            for (NamedExpression item : ((LogicalProject<?>) plan).getProjects()) {
                if (!childrenExprIds.contains(item.getExprId()) || isSameValueAsChildSlot(item)) {
                    continue;
                }
                Assertions.fail("project reuses the exprId " + item.getExprId()
                        + " of its child, but the item describes another value: " + item);
            }
        }
        plan.children().forEach(ConstantPropagationFreshExprIdTest::assertNoShadowProject);
    }

    private static boolean isSameValueAsChildSlot(NamedExpression item) {
        if (item instanceof SlotReference) {
            return true;
        }
        // Alias(X, slotX) just wraps the child slot of the same exprId, its value and attributes are unchanged
        if (!(item instanceof Alias) || !(item.child(0) instanceof SlotReference)) {
            return false;
        }
        return ((SlotReference) item.child(0)).getExprId().equals(item.getExprId());
    }
}
