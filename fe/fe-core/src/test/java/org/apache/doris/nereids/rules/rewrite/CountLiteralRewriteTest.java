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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * CountLiteralRewriteTest
 */
class CountLiteralRewriteTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int, sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testCountLiteral() {
        PlanChecker.from(connectContext)
                .analyze("select count(1) as c from student group by id")
                .rewrite()
                .matches(logicalAggregate()
                        .when(agg -> agg.getOutputExpressions().stream()
                                .allMatch(expr -> expr.anyMatch(c -> !(c instanceof Count) || ((Count) c).isCountStar()))))
                .printlnTree();
        PlanChecker.from(connectContext)
                .analyze("select count(1), sum(id) from student")
                .rewrite()
                .matches(logicalAggregate()
                        .when(agg -> agg.getOutputExpressions().stream()
                                .allMatch(expr -> expr.anyMatch(c -> !(c instanceof Count) || ((Count) c).isCountStar()))))
                .printlnTree();
    }

    @Test
    void testCountNull() {
        PlanChecker.from(connectContext)
                .analyze("select count(null) as c from student group by id")
                .rewrite()
                .matches(logicalAggregate().when(agg -> agg.getExpressions().stream().noneMatch(Count.class::isInstance)))
                .printlnTree();
        PlanChecker.from(connectContext)
                .analyze("select count(null) as c, sum(id) from student")
                .rewrite()
                .matches(logicalAggregate().when(agg -> agg.getExpressions().stream().noneMatch(Count.class::isInstance)))
                .printlnTree();
    }

    @Test
    void testCountNullIsNotCountStar() {
        // count(null) should NOT be treated as count(*)
        Count countNull = new Count(NullLiteral.INSTANCE);
        Assertions.assertFalse(countNull.isCountStar(),
                "count(null) should not be count star, because count(null) is always 0");

        // typed null literal: count(CAST(null AS BIGINT)) should NOT be count(*)
        Count countTypedNull = new Count(new NullLiteral(BigIntType.INSTANCE));
        Assertions.assertFalse(countTypedNull.isCountStar(),
                "count(typed null) should not be count star");

        // count(distinct null) should NOT be treated as count(*)
        Count countDistinctNull = new Count(true, NullLiteral.INSTANCE);
        Assertions.assertFalse(countDistinctNull.isCountStar(),
                "count(distinct null) should not be count star");

        // count(1) should be treated as count(*)
        Count countOne = new Count(new BigIntLiteral(1));
        Assertions.assertTrue(countOne.isCountStar(),
                "count(1) should be count star");

        // count(*) should be treated as count(*)
        Count countStar = new Count();
        Assertions.assertTrue(countStar.isCountStar(),
                "count(*) should be count star");
    }
}
