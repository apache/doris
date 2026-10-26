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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The project which ConstantPropagation materializes above an aggregate must not re-label a column which the plan
 * below defines with another value.
 *
 * <p>A constant grouping key which the aggregate keeps is passed through as the slot of the aggregate, and a
 * constant grouping key which the aggregate removes keeps the exprId it already had, so nothing below defines that
 * exprId any more. The project item used to reuse the exprId of the removed aggregate output while other rules
 * later brought a subtree which outputs that exprId back below it, and rules which compare exprIds only, e.g.
 * EliminateUnnecessaryProject, then removed the constant project. RQG case 11000258898 reached such a plan and the
 * last AdjustNullable pass reported "AdjustNullable convert slot 2004-04-28#xx from not-nullable to nullable".
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

    private static final String SQL_11000259585 = "select * from ((select 'vZRgkfajsD', 'cKToNmsxok' except (select col_char_25__undef_signed_not_null, col_char_40__undef_signed from table_100_undef_partitions2_keys3_properties4_distributed_by54 where col_date_undef_signed = '2016-09-20' order by 1, 2 desc limit 1, 8) union distinct select col_varchar_25__undef_signed, col_char_25__undef_signed from table_100_undef_partitions2_keys3_properties4_distributed_by53 where col_tinyint_undef_signed_not_null = 4.4929476592789594 intersect distinct select col_date_undef_signed_not_null, col_date_undef_signed_not_null from table_100_undef_partitions2_keys3_properties4_distributed_by52 where col_decimal_12_2__undef_signed = 94.0212) as t1);";

    private static final String SQL_MATERIALIZED_CTE = "WITH c AS (SELECT k, v, count(*) AS cnt"
            + " FROM constant_propagation_fresh_expr_id_t WHERE k = 5 AND v = 7 GROUP BY k, v)"
            + " SELECT l.k, l.v, l.cnt, r.k, r.v, r.cnt FROM c l CROSS JOIN c r";

    private static final String SQL_MATERIALIZED_CTE_CONSTANT_COLUMN
            = "WITH c AS (SELECT v, k + 0 AS x, count(*) AS cnt"
            + " FROM constant_propagation_fresh_expr_id_t WHERE k = 5 GROUP BY v, k)"
            + " SELECT l.v, l.x, l.cnt, r.v, r.x, r.cnt FROM c l CROSS JOIN c r";

    private static final String SQL_MATERIALIZED_CTE_REPEATED_SLOT
            = "WITH c AS (SELECT k, v, v, count(*) AS cnt"
            + " FROM constant_propagation_fresh_expr_id_t WHERE k = 5 AND v = 7 GROUP BY k, v)"
            + " SELECT l.*, r.* FROM c l CROSS JOIN c r";

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
    void testConstantAggregateProjectKeepsTheExprId() {
        // run this rule alone, so the project it materializes above the aggregate is still part of the plan
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze("select k, v from constant_propagation_fresh_expr_id_t where k = 5 and v = 7 group by k, v")
                .applyCustom(new ConstantPropagation());
        Plan plan = checker.getPlan();
        LogicalProject<?> constantProject = findConstantProject(plan);
        Assertions.assertNotNull(constantProject, plan.treeString());
        List<NamedExpression> items = constantProject.getProjects();
        // the kept grouping key is still materialized as a constant above the aggregate, with the exprId of the
        // aggregate output: no fresh exprId and no refresh of the plan above
        NamedExpression keptKey = items.get(0);
        Assertions.assertTrue(keptKey instanceof Alias, plan.treeString());
        Assertions.assertEquals(new IntegerLiteral(5), ((Alias) keptKey).child());
        Assertions.assertTrue(constantProject.child().getOutput().stream()
                        .anyMatch(slot -> slot.getExprId().equals(keptKey.getExprId())),
                "the kept grouping key keeps the exprId of the aggregate output:\n" + plan.treeString());
        // the removed grouping key is materialized with the exprId it already had as well
        NamedExpression removedKey = items.get(1);
        Assertions.assertTrue(removedKey instanceof Alias, plan.treeString());
        Assertions.assertEquals(new IntegerLiteral(7), ((Alias) removedKey).child());
        // the plan above references the outputs of the project, unchanged
        Set<ExprId> projectOutputIds = constantProject.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toSet());
        Set<ExprId> rootOutputIds = plan.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toSet());
        Assertions.assertTrue(rootOutputIds.containsAll(projectOutputIds),
                "the plan above does not reference the project outputs:\n" + plan.treeString());
    }

    // The two RQG reproductions of the master version (testRqgCase11000258898 and testRqgCase11000259585) are
    // not ported: on branch-4.2 both queries already trip "AdjustNullable convert slot 2004-04-28#55 from
    // not-nullable to nullable" (and "cKToNmsxok#45") during the rewrite on the pristine branch, i.e. before this
    // change, and the failure is unchanged after it. ConstantPropagation does not modify the plans of these
    // queries on this branch at all (dumped with the rule applied alone: the plan is identical), so the failure
    // comes from another rule here, while on master the rule does rewrite them. The tables those queries need
    // are still created below, so the SQL constants stay usable if the branch is fixed later.

    @Test
    void testMaterializedCteConsumerMapsStayValid() throws Exception {
        // the producer body and the consumers are rewritten in different subtree contexts and the consumers are
        // rewritten first, so the fresh exprIds of the producer outputs must not cross the CTE boundary
        Set<ExprId> analyzedProducerOutputIds = cteProducerOutputIds(
                PlanChecker.from(connectContext).analyze(SQL_MATERIALIZED_CTE).getPlan());
        Plan plan = PlanChecker.from(connectContext).analyze(SQL_MATERIALIZED_CTE).rewrite().getPlan();
        assertCteConsumerMapsStayValid(plan);
        // constant propagation keeps the exprIds of the producer outputs, so they are the same as in the analyzed
        // plan and the consumer maps still resolve; a rule which renames them has to refresh the maps, which the
        // assertCteConsumerMapsStayValid above checks
        Set<ExprId> rewrittenProducerOutputIds = cteProducerOutputIds(plan);
        Assertions.assertEquals(analyzedProducerOutputIds.size(), rewrittenProducerOutputIds.size(),
                plan.treeString());
        Assertions.assertEquals(analyzedProducerOutputIds, rewrittenProducerOutputIds, plan.treeString());
        // the query has to stay plannable
        PlanChecker.from(connectContext).checkPlannerResult(SQL_MATERIALIZED_CTE);
    }

    @Test
    void testMaterializedCteWithRepeatedOutputSlot() {
        // a CTE may expose the same slot more than once (LogicalCTEConsumer is multimap based), and the lookup of
        // the producer slots must tolerate that instead of failing on the duplicated exprId
        Set<ExprId> analyzedProducerOutputIds = cteProducerOutputIds(
                PlanChecker.from(connectContext).analyze(SQL_MATERIALIZED_CTE_REPEATED_SLOT).getPlan());
        Plan plan = PlanChecker.from(connectContext)
                .analyze(SQL_MATERIALIZED_CTE_REPEATED_SLOT).rewrite().getPlan();
        assertCteConsumerMapsStayValid(plan);
        Assertions.assertEquals(analyzedProducerOutputIds.size(), cteProducerOutputIds(plan).size(), plan.treeString());
        PlanChecker.from(connectContext).checkPlannerResult(SQL_MATERIALIZED_CTE_REPEATED_SLOT);
    }

    private static Set<ExprId> cteProducerOutputIds(Plan plan) {
        Set<ExprId> producerOutputExprIds = new java.util.HashSet<>();
        plan.foreach(node -> {
            if (node instanceof LogicalCTEProducer) {
                ((LogicalCTEProducer<?>) node).getOutput().forEach(slot -> producerOutputExprIds.add(slot.getExprId()));
            }
            return false;
        });
        Assertions.assertFalse(producerOutputExprIds.isEmpty(), plan.treeString());
        return producerOutputExprIds;
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
        // `k + 0` is uniform (k = 5), so it folds to a constant, and it is an output column of the CTE producer:
        // whether the rule materializes the constant or leaves the column alone, the consumers have to keep
        // mapping to an output which the producer really exposes
        Set<ExprId> analyzedProducerOutputIds = cteProducerOutputIds(
                PlanChecker.from(connectContext).analyze(SQL_MATERIALIZED_CTE_CONSTANT_COLUMN).getPlan());
        Plan plan = PlanChecker.from(connectContext)
                .analyze(SQL_MATERIALIZED_CTE_CONSTANT_COLUMN).rewrite().getPlan();
        assertCteConsumerMapsStayValid(plan);
        // the producer keeps its three output columns, whether they are rewritten in place or renamed
        Assertions.assertEquals(analyzedProducerOutputIds.size(), cteProducerOutputIds(plan).size(), plan.treeString());
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

}
