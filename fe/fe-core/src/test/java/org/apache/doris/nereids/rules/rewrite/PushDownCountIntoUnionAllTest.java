package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class PushDownCountIntoUnionAllTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.t1 (\n"
                + "id int not null,\n"
                + "a varchar(128),\n"
                + "b int,c int)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testPushCountStar() {
        String sql = "select count(1) from (select id,a from t1 union all select id,a from t1 where id>10) t;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                              logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class))
                                      ,logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)))
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
        String sql2 = "select count(*) from (select id,a from t1 union all select id,a from t1 where id>10) t;";
        PlanChecker.from(connectContext)
                .analyze(sql2)
                .rewrite()
                .matches(
                        logicalAggregate(
                                logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class))
                                        ,logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)))
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
    }

    @Test
    void testPushCountColumn() {
        String sql = "select count(id) from (select id,a from t1 union all select id,a from t1 where id>10) t;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                                logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class))
                                        ,logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)))
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
    }

    @Test
    void testPushCountColumnWithGroupBy() {
        String sql = "select count(id),a from (select id,a from t1 union all select id,a from t1 where id>10) t group by a;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                                logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)
                                        && agg.getGroupByExpressions().size() == 1)
                                        ,logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)
                                                && agg.getGroupByExpressions().size() == 1))
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
    }

    @Test
    void testNotPushCountBecauseOtherAggFunc() {
        String sql = "select count(1), sum(id) from (select id,a from t1 union all select id,a from t1 where id>10) t;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(
                        logicalAggregate(
                                logicalUnion(logicalAggregate(), logicalAggregate())
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
    }

    @Test
    void testNotPushCountBecauseUnion() {
        String sql = "select count(1), sum(id) from (select id,a from t1 union select id,a from t1 where id>10) t;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(
                        logicalAggregate(
                                logicalUnion(logicalAggregate(), logicalAggregate())
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );

        String sql2 = "select count(1), sum(id) from (select id,a from t1 union all select id,a from t1 where id>10 union all select 1,3) t;";
        PlanChecker.from(connectContext)
                .analyze(sql2)
                .rewrite()
                .nonMatch(
                        logicalAggregate(
                                logicalUnion(logicalAggregate(), logicalAggregate())
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );

        String sql3 = "select count(1) from (select 3,6 union all select 1,3) t;";
        PlanChecker.from(connectContext)
                .analyze(sql3)
                .rewrite()
                .nonMatch(
                        logicalAggregate(
                                logicalUnion(logicalAggregate(), logicalAggregate())
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
    }
}
