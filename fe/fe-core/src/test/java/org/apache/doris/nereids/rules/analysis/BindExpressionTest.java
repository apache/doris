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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class BindExpressionTest extends TestWithFeService implements GeneratedPlanPatterns {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTables(
                "CREATE TABLE t1 (col1 date, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");",
                "CREATE TABLE t2 (col1 date, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");"
        );
    }

    @Test
    void testJoin() {
        for (JoinType joinType : JoinType.values()) {
            String sql = String.format("select * from t1 %s t2 on t1.col2 = t2.col2",
                    joinType.toString().replace("_", " "));
            if (joinType.isCrossJoin()) {
                sql = String.format("select * from t1 %s t2",
                        joinType.toString().replace("_", " "));
            }
            if (joinType.isNullAwareLeftAntiJoin() || joinType.isAsofJoin()) {
                continue;
            }
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .nonMatch(any()
                            .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
        }
    }

    @Test
    void testAggHaving() {
        String sql = "select sum(col2) from t1 group by col1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));

        sql = "select sum(col2) from t1 group by col2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));

        sql = "select sum(col2) from t1 group by col2, col1 having col1 > 0";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));

    }

    @Test
    void testFilter() {
        String sql = "select * from t1 where t1.col1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));

    }

    @Test
    void testSubquery() {
        String sql = "select * from t1 where t1.col2 = (select sum(col2) from t2)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));

    }

    @Test
    void testFilterSort() {
        String sql = "select * from t1 where t1.col2 = 1 order by col2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
        sql = "select * from t1 where t1.col2 = 1 order by col1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));

    }

    @Test
    void testOneRealation() {
        String sql = "select 1 + 2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Test
    void testIcebergMergeUsesPositionForQuotedOperationTargetColumn() {
        List<NamedExpression> output = ImmutableList.of(
                new Alias(new TinyIntLiteral((byte) 3), IcebergMergeOperation.OPERATION_COLUMN),
                new SlotReference(Column.ICEBERG_ROWID_COL, StringType.INSTANCE),
                new Alias(new IntegerLiteral(7), IcebergMergeOperation.OPERATION_COLUMN));
        List<Column> columns = ImmutableList.of(
                new Column(IcebergMergeOperation.OPERATION_COLUMN, PrimitiveType.SMALLINT));

        List<NamedExpression> coerced = BindExpression.coerceIcebergMergeOutput(
                columns, output, true);

        Assertions.assertEquals(3, coerced.size());
        Assertions.assertEquals(SmallIntType.INSTANCE, coerced.get(2).getDataType());
        Assertions.assertEquals(IcebergMergeOperation.OPERATION_COLUMN, coerced.get(2).getName());
    }

    @Test
    void testIcebergMergeChecksLegacyVariantInQuotedOperationTargetColumn() {
        List<NamedExpression> output = ImmutableList.of(
                new Alias(new TinyIntLiteral((byte) 3), IcebergMergeOperation.OPERATION_COLUMN),
                new SlotReference(Column.ICEBERG_ROWID_COL, StringType.INSTANCE),
                new SlotReference(IcebergMergeOperation.OPERATION_COLUMN, VariantType.INSTANCE));
        List<Column> columns = ImmutableList.of(new Column(
                IcebergMergeOperation.OPERATION_COLUMN,
                IcebergUtils.icebergTypeToDorisType(
                        org.apache.iceberg.types.Types.VariantType.get(), false, false)));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> BindExpression.coerceIcebergMergeOutput(columns, output, true));
        Assertions.assertTrue(exception.getMessage().contains("legacy Doris VARIANT"));
        Assertions.assertTrue(exception.getMessage().contains("operation"));
    }

    @Test
    void testSetOperation() {
        String sql = "select * from t1 union all select * from t2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
        sql = "select * from t1 union select * from t2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
        sql = "select * from t1 intersect select * from t2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Test
    void testRepeat() {
        String sql = "select repeat(\"a\", 3)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .nonMatch(any()
                        .when(e -> e.getExpressions().stream().anyMatch(Expression::hasUnbound)));
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }

}
