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

import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class PushCountIntoUnionAllTest extends TestWithFeService implements MemoPatternMatchSupported {
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
        String sql = "select id,count(1) from (select id,a from t1 union all select id,a from t1 where id>10) t group by id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                              logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)),
                                      logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)))
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
        String sql2 = "select id,count(*) from (select id,a from t1 union all select id,a from t1 where id>10) t group by id;";
        PlanChecker.from(connectContext)
                .analyze(sql2)
                .rewrite()
                .matches(
                        logicalAggregate(
                                logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)),
                                        logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)))
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
    }

    @Test
    void testPushCountStarNoOtherColumn() {
        String sql = "select count(1) from (select id,a from t1 union all select id,a from t1 where id>10) t;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                                logicalUnion(logicalAggregate(), logicalAggregate())
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
        String sql2 = "select count(*) from (select id,a from t1 union all select id,a from t1 where id>10) t;";
        PlanChecker.from(connectContext)
                .analyze(sql2)
                .rewrite()
                .matches(
                        logicalAggregate(
                                logicalUnion(logicalAggregate(), logicalAggregate())
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
                                logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)),
                                        logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)))
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
                                        && agg.getGroupByExpressions().size() == 1),
                                        logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)
                                                && agg.getGroupByExpressions().size() == 1))
                        ).when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Sum0.class))
                );
    }

    @Test
    void testPush2CountColumn() {
        String sql = "select count(id), count(b), a from (select id,b,a from t1 union all select id,a,b from t1 where id>10) t group by a;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                                logicalUnion(logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)
                                                && agg.getGroupByExpressions().size() == 1),
                                        logicalAggregate().when(agg -> ExpressionUtils.containsType(agg.getOutputExpressions(), Count.class)
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
    }
}
