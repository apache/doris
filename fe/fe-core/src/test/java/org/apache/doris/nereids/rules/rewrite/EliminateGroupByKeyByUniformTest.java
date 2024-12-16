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

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class EliminateGroupByKeyByUniformTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.eli_gbk_by_uniform_t(a int null, b int not null,"
                + "c varchar(10) null, d date, dt datetime)\n"
                + "distributed by hash(a) properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testEliminateByFilter() {
        PlanChecker.from(connectContext)
                .analyze("select a, min(a), sum(a),b from eli_gbk_by_uniform_t where a = 1 group by a,b")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")));

    }

    @Test
    void testNotEliminateWhenOnlyOneGbyKey() {
        PlanChecker.from(connectContext)
                .analyze("select a, min(a), sum(a) from eli_gbk_by_uniform_t where a = 1 group by a")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("a")));

    }

    @Test
    void testEliminateByProjectConst() {
        PlanChecker.from(connectContext)
                .analyze("select  sum(c1), c2 from (select a c1,1 c2, d c3 from eli_gbk_by_uniform_t) t group by c2,c3 ")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("c3")));
    }

    @Test
    void testEliminateByProjectUniformSlot() {
        PlanChecker.from(connectContext)
                .analyze("select  max(c3), c1,c2,c3 from (select a c1,1 c2, d c3 from eli_gbk_by_uniform_t where a=1) t group by c1,c2,c3")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("c3")));
    }

    @Test
    void testEliminateDate() {
        PlanChecker.from(connectContext)
                .analyze("select  d, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where d = '2023-01-06' group by d,a")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("a")));
    }

    @Test
    void testSaveOneExpr() {
        PlanChecker.from(connectContext)
                .analyze("select  a, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where a = 1 and b=100 group by a, b,'abc'")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("a")));
    }

    @Test
    void testSaveOneExprProjectConst() {
        PlanChecker.from(connectContext)
                .analyze("select  c2 from (select a c1,1 c2, 3 c3 from eli_gbk_by_uniform_t) t group by c2,c3 order by 1;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("c2")));
    }

    @Test
    void testNotRewriteWhenHasRepeat() {
        PlanChecker.from(connectContext)
                .analyze("select  c2 from (select a c1,1 c2, 3 c3 from eli_gbk_by_uniform_t) t group by grouping sets((c2),(c3)) order by 1;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 3));
    }

    @Test
    void testInnerJoin() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b,t2.b from eli_gbk_by_uniform_t t1 inner join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t2.b,t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }

    @Test
    void testLeftJoinOnConditionNotRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b,t2.b from eli_gbk_by_uniform_t t1 left join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t2.b,t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 3));
    }

    @Test
    void testLeftJoinWhereConditionRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b,t2.b from eli_gbk_by_uniform_t t1 left join eli_gbk_by_uniform_t t2 on t1.b=t2.b where t1.b=100 group by t1.b,t2.b,t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 2));
    }

    @Test
    void testRightJoinOnConditionNullableSideFilterNotRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b,t2.b from eli_gbk_by_uniform_t t1 right join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t2.b,t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 3));
    }

    @Test
    void testRightJoinOnConditionNonNullableSideFilterNotRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b,t2.b from eli_gbk_by_uniform_t t1 right join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b,t2.b,t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 3));
    }

    @Test
    void testRightJoinWhereConditionToInnerRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b,t2.b from eli_gbk_by_uniform_t t1 right join eli_gbk_by_uniform_t t2 on t1.b=t2.b where t1.b=100 group by t1.b,t2.b,t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }

    @Test
    void testLeftSemiJoinWhereConditionRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b from eli_gbk_by_uniform_t t1 left semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b,t1.a")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }

    @Test
    void testLeftSemiJoinRetainOneSlotInGroupBy() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b from eli_gbk_by_uniform_t t1 left semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }

    @Test
    void testRightSemiJoinWhereConditionRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t2.b from eli_gbk_by_uniform_t t1 right semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t2.b,t2.a")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }

    @Test
    void testRightSemiJoinRetainOneSlotInGroupBy() {
        PlanChecker.from(connectContext)
                .analyze("select  t2.b from eli_gbk_by_uniform_t t1 right semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t2.b")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }

    @Test
    void testLeftAntiJoinOnConditionNotRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b from eli_gbk_by_uniform_t t1 left anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t1.a")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 2));
    }

    @Test
    void testLeftAntiJoinWhereConditionRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t1.b from eli_gbk_by_uniform_t t1 left anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b where t1.b=100 group by t1.b,t1.c")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }

    @Test
    void testRightAntiJoinOnConditionNotRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t2.b from eli_gbk_by_uniform_t t1 right anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t2.b,t2.a")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 2));
    }

    @Test
    void testRightAntiJoinWhereConditionRewrite() {
        PlanChecker.from(connectContext)
                .analyze("select  t2.b from eli_gbk_by_uniform_t t1 right anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b where t2.b=100 group by t2.b,t2.c")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1));
    }
}
