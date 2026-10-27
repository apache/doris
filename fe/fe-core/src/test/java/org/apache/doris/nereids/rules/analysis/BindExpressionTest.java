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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.NereidsException;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayHourAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourSecondSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MicroSecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MicroSecondsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearMonthAdd;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

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
    void testGroupByOrdinalIsNotNarrowed() {
        // getIntValue() is getNumber().intValue(), so a BIGINT or LARGEINT ordinal was truncated
        // to its low 32 bits before the `>= 1 && <= selectItems` test. 4294967297 is 2^32 + 1 and
        // 18446744073709551617 is 2^64 + 1: both truncated to 1 and silently bound to the first
        // select item, while the plainly out-of-range 3 did not. The LARGEINT narrows to 1 through
        // getLongValue() as well, so it pins the width-independent accessor, not merely a wider one.
        String outOfRange = "select col1, count(*) from t1 group by 3";

        // As a constant, the literal leaves col1 ungrouped. checkPlannerResult surfaces that as
        // NereidsException, and every out-of-range ordinal must fail for that same reason.
        NereidsException expected = Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(outOfRange),
                "an ordinal past the end of the select list must not bind");
        for (String ordinal : new String[] {"4294967297", "18446744073709551617"}) {
            String wrapsToOne = "select col1, count(*) from t1 group by " + ordinal;
            NereidsException actual = Assertions.assertThrows(NereidsException.class,
                    () -> PlanChecker.from(connectContext).checkPlannerResult(wrapsToOne),
                    ordinal + " must not be narrowed into range");
            Assertions.assertEquals(expected.getMessage(), actual.getMessage(),
                    ordinal + " must be rejected for the same reason as the out-of-range ordinal");
        }
    }

    @Test
    void testOrderByOrdinalIsNotNarrowed() {
        // bindWithOrdinal also serves ORDER BY, on a plain select and on a set operation. There an
        // out-of-range ordinal does not throw -- it stays a constant sort key -- so the check is on
        // the analyzed plan: a wide ordinal must never turn into a sort on a column.
        String[] queries = {
                "select col1, col2 from t1 order by %s",
                "select col1, col2 from t1 union all select col1, col2 from t2 order by %s",
        };
        for (String query : queries) {
            // The check can tell the two apart: a real ordinal does sort on a column.
            PlanChecker.from(connectContext)
                    .analyze(String.format(query, "1"))
                    .matches(logicalSort().when(BindExpressionTest::sortsOnASlot));
            for (String ordinal : new String[] {"4294967297", "18446744073709551617"}) {
                PlanChecker.from(connectContext)
                        .analyze(String.format(query, ordinal))
                        .nonMatch(logicalSort().when(BindExpressionTest::sortsOnASlot));
            }
        }
    }

    private static boolean sortsOnASlot(LogicalSort<?> sort) {
        return sort.getOrderKeys().stream().anyMatch(key -> key.getExpr() instanceof Slot);
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
    void testCompoundIntervalArithmetic() {
        PlanChecker.from(connectContext)
                .analyze("select cast(col1 as datetime) + interval '1-2' year_month from t1")
                .matches(any().when(plan -> plan.getExpressions().stream()
                        .anyMatch(expression -> expression.anyMatch(YearMonthAdd.class::isInstance))));

        PlanChecker.from(connectContext)
                .analyze("select interval '1 5' day_hour + cast(col1 as datetime) from t1")
                .matches(any().when(plan -> plan.getExpressions().stream()
                        .anyMatch(expression -> expression.anyMatch(DayHourAdd.class::isInstance))));

        PlanChecker.from(connectContext)
                .analyze("select cast(col1 as datetime) - interval '2 30' hour_second from t1")
                .matches(any().when(plan -> plan.getExpressions().stream()
                        .anyMatch(expression -> expression.anyMatch(HourSecondSub.class::isInstance))));
    }

    @Test
    void testSimpleIntervalArithmeticBinding() {
        PlanChecker.from(connectContext)
                .analyze("select cast(col1 as datetime) + interval 1 microsecond from t1")
                .matches(any().when(plan -> plan.getExpressions().stream()
                        .anyMatch(expression -> expression.anyMatch(MicroSecondsAdd.class::isInstance))));

        PlanChecker.from(connectContext)
                .analyze("select cast(col1 as datetime) - interval 1 microsecond from t1")
                .matches(any().when(plan -> plan.getExpressions().stream()
                        .anyMatch(expression -> expression.anyMatch(MicroSecondsSub.class::isInstance))));
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

    @Test
    void testRowLevelMergeOutputIsCoercedByPosition() {
        // A data column named like the operation routing column is matched by its position, so it is still cast to
        // its target type. Matching by name skipped every cast of such an output.
        List<NamedExpression> output = ImmutableList.of(
                new Alias(new TinyIntLiteral((byte) 3), MergeOperation.OPERATION_COLUMN),
                new SlotReference(Column.ICEBERG_ROWID_COL, StringType.INSTANCE),
                new Alias(new IntegerLiteral(7), MergeOperation.OPERATION_COLUMN));
        List<Column> columns = ImmutableList.of(
                new Column(MergeOperation.OPERATION_COLUMN, PrimitiveType.SMALLINT));

        List<NamedExpression> coerced = BindExpression.coerceMergeOutput(columns, output);

        Assertions.assertEquals(3, coerced.size());
        Assertions.assertEquals(SmallIntType.INSTANCE, coerced.get(2).getDataType());
        Assertions.assertEquals(MergeOperation.OPERATION_COLUMN, coerced.get(2).getName());

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> BindExpression.coerceMergeOutput(columns, output.subList(0, 2)));
        Assertions.assertTrue(exception.getMessage().contains("not aligned"), exception.getMessage());
    }

    @Test
    void testMergeAcceptsMatchingNestedSpatialDataWrites() {
        for (DataType spatial : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            ArrayType array = ArrayType.of(spatial);
            LogicalOneRowRelation input = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(
                    new Alias(new NullLiteral(array), "shapes")));
            LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                    new Alias(new TinyIntLiteral((byte) 1), MergeOperation.OPERATION_COLUMN),
                    new Alias(new StringLiteral("row"), Column.ICEBERG_ROWID_COL),
                    new Alias(input.getOutput().get(0), "shapes")), input);
            List<Column> columns = ImmutableList.of(new Column("shapes", array.toCatalogDataType()));
            for (boolean writesDataFiles : new boolean[] {false, true}) {
                LogicalExternalRowLevelMergeSink<?> sink = new LogicalExternalRowLevelMergeSink<>(
                        Mockito.mock(ExternalDatabase.class), Mockito.mock(ExternalTable.class),
                        "schema-1", columns, ImmutableList.of(), writesDataFiles, true,
                        Optional.empty(), Optional.empty(), source);
                Assertions.assertDoesNotThrow(
                        () -> PlanChecker.from(connectContext, sink).applyTopDown(new BindExpression()));
            }
        }
    }

    @Test
    void testMergeNormalizesProvenSpatialNullsBeforeCoercion() {
        for (DataType target : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType nullType : ImmutableList.of(IntegerType.INSTANCE, new GeometryType("EPSG:3857"),
                    new GeographyType("OGC:CRS84", "vincenty"))) {
                LogicalOneRowRelation input = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(
                        new Alias(new IntegerLiteral(1), "id")));
                LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                        new Alias(new TinyIntLiteral((byte) 3), MergeOperation.OPERATION_COLUMN),
                        new Alias(new StringLiteral("row"), Column.ICEBERG_ROWID_COL),
                        input.getOutput().get(0),
                        new Alias(new Cast(new NullLiteral(), nullType), "shape")), input);
                List<Column> columns = ImmutableList.of(new Column("id", PrimitiveType.INT),
                        new Column("shape", target.toCatalogDataType()));
                LogicalExternalRowLevelMergeSink<?> sink = new LogicalExternalRowLevelMergeSink<>(
                        Mockito.mock(ExternalDatabase.class), Mockito.mock(ExternalTable.class),
                        "schema-1", columns, ImmutableList.of(), true, true,
                        Optional.empty(), Optional.empty(), source);
                LogicalExternalRowLevelMergeSink<?> bound = (LogicalExternalRowLevelMergeSink<?>) PlanChecker
                        .from(connectContext, sink).applyTopDown(new BindExpression()).getPlan();
                Assertions.assertEquals(target, bound.getOutputExprs().get(3).getDataType());
                for (int i = 0; i < 3; i++) {
                    Assertions.assertEquals(source.getOutput().get(i), bound.getOutputExprs().get(i));
                }
            }
        }
    }

    @Test
    void testMergeSpatialNullsPreserveRoutingPassthroughAndOrdinaryCasts() {
        for (DataType target : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            LogicalOneRowRelation input = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(
                    new Alias(new NullLiteral(IntegerType.INSTANCE), "n"),
                    new Alias(new StringLiteral("kept"), "reserved")));
            LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                    new Alias(new TinyIntLiteral((byte) 3), MergeOperation.OPERATION_COLUMN),
                    new Alias(new StringLiteral("row"), Column.ICEBERG_ROWID_COL),
                    input.getOutput().get(0), input.getOutput().get(1),
                    new Alias(input.getOutput().get(0), MergeOperation.OPERATION_COLUMN)), input);
            Column hidden = new Column("hidden", PrimitiveType.INT);
            hidden.setIsVisible(false);
            Column reserved = new Column("reserved", PrimitiveType.STRING);
            reserved.setIsVisible(false);
            reserved.setReservedPassthrough(true);
            List<Column> columns = ImmutableList.of(hidden, new Column("n", PrimitiveType.BIGINT), reserved,
                    new Column(MergeOperation.OPERATION_COLUMN, target.toCatalogDataType()));
            LogicalExternalRowLevelMergeSink<?> sink = new LogicalExternalRowLevelMergeSink<>(
                    Mockito.mock(ExternalDatabase.class), Mockito.mock(ExternalTable.class),
                    "schema-1", columns, ImmutableList.of(), true, true,
                    Optional.empty(), Optional.empty(), source);
            LogicalExternalRowLevelMergeSink<?> bound = (LogicalExternalRowLevelMergeSink<?>) PlanChecker
                    .from(connectContext, sink).applyTopDown(new BindExpression()).getPlan();
            List<NamedExpression> output = bound.getOutputExprs();
            for (int i : new int[] {0, 1, 3}) {
                Assertions.assertEquals(source.getOutput().get(i), output.get(i));
            }
            Assertions.assertEquals(BigIntType.INSTANCE, output.get(2).getDataType());
            Expression ordinary = ((LogicalProject<?>) bound.child()).getProjects().get(2).child(0);
            Assertions.assertInstanceOf(Cast.class, ordinary);
            Assertions.assertEquals(IntegerType.INSTANCE, ordinary.child(0).getDataType());
            Assertions.assertEquals(target, output.get(4).getDataType());
            Assertions.assertNotEquals(output.get(2).getExprId(), output.get(4).getExprId());
            LogicalExternalRowLevelMergeSink<?> rebound = (LogicalExternalRowLevelMergeSink<?>) PlanChecker
                    .from(connectContext, bound).applyTopDown(new BindExpression()).getPlan();
            Assertions.assertEquals(output, rebound.getOutputExprs());
        }
    }

    @Test
    void testMergeRejectsMismatchedNestedSpatialDataWrites() {
        for (DataType spatial : ImmutableList.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            ArrayType sourceType = ArrayType.of(spatial);
            LogicalEmptyRelation input = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(
                    new SlotReference("shapes", sourceType)));
            for (DataType target : ImmutableList.of(new GeometryType("EPSG:3857"),
                    new GeographyType("OGC:CRS84", "vincenty"))) {
                ArrayType targetType = ArrayType.of(target);
                // MERGE already projects target casts; validate the original typed slot below it.
                LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                        new Alias(new TinyIntLiteral((byte) 3), MergeOperation.OPERATION_COLUMN),
                        new Alias(new StringLiteral("row"), Column.ICEBERG_ROWID_COL),
                        new Alias(new Cast(input.getOutput().get(0), targetType), "shapes")), input);
                List<Column> columns = ImmutableList.of(new Column("shapes",
                        targetType.toCatalogDataType()));
                LogicalExternalRowLevelMergeSink<?> sink = new LogicalExternalRowLevelMergeSink<>(
                        Mockito.mock(ExternalDatabase.class), Mockito.mock(ExternalTable.class),
                        "schema-1", columns, ImmutableList.of(), true, true,
                        Optional.empty(), Optional.empty(), source);
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> PlanChecker.from(connectContext, sink).applyTopDown(new BindExpression()));
                String mismatch = spatial.getClass() != target.getClass() ? "type mismatch"
                        : spatial instanceof GeometryType ? "CRS mismatch" : "algorithm mismatch";
                Assertions.assertTrue(exception.getMessage().contains(mismatch), exception.getMessage());
            }
        }
    }

    @Test
    void testRowLevelMergeValidatesSpatialTargetNamedLikeTheOperationColumn() {
        GeometryType projected = new GeometryType("EPSG:3857");
        GeometryType target = new GeometryType("OGC:CRS84");
        LogicalOneRowRelation input = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(
                new Alias(new NullLiteral(projected), "source_shape")));
        // The output slot carries the target type, but the projection still contains the
        // original CRS under the target cast. Check data position 2 even when named operation.
        LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                new Alias(new TinyIntLiteral((byte) 3), MergeOperation.OPERATION_COLUMN),
                new Alias(new StringLiteral("row"), Column.ICEBERG_ROWID_COL),
                new Alias(new Cast(new SlotReference("shape", projected), target),
                        MergeOperation.OPERATION_COLUMN)), input);
        List<Column> columns = ImmutableList.of(new Column(MergeOperation.OPERATION_COLUMN,
                ScalarType.createGeometryType()));
        List<NamedExpression> output = source.getOutput().stream()
                .map(NamedExpression.class::cast).collect(ImmutableList.toImmutableList());

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> BindExpression.validateMergeSpatialTypes(columns, source, output));
        Assertions.assertTrue(exception.getMessage().contains("CRS mismatch"), exception.getMessage());

        List<Column> matchingColumns = ImmutableList.of(new Column(MergeOperation.OPERATION_COLUMN,
                ScalarType.createGeometryType("EPSG:3857")));
        LogicalProject<?> matchingSource = new LogicalProject<>(ImmutableList.of(
                source.getProjects().get(0), source.getProjects().get(1),
                new Alias(new SlotReference("shape", projected), MergeOperation.OPERATION_COLUMN)), input);
        Assertions.assertDoesNotThrow(
                () -> BindExpression.validateMergeSpatialTypes(matchingColumns, matchingSource, output));
    }

    @Test
    void testRowLevelMergeValidatesVariantTargetNamedLikeTheOperationColumn() {
        // The Variant target named like the operation routing column is traced at its own position.
        LogicalOneRowRelation input = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(
                new Alias(new NullLiteral(ConnectorComputeVariantType.INSTANCE), "source_payload")));
        Slot sourcePayload = input.getOutput().get(0);
        LogicalProject<?> source = new LogicalProject<>(ImmutableList.of(
                new Alias(new TinyIntLiteral((byte) 3), MergeOperation.OPERATION_COLUMN),
                new Alias(new StringLiteral("row"), Column.ICEBERG_ROWID_COL),
                new Alias(new If(BooleanLiteral.TRUE, new Cast(sourcePayload, IntegerType.INSTANCE),
                        new IntegerLiteral(1)), MergeOperation.OPERATION_COLUMN)), input);
        List<Column> columns = ImmutableList.of(new Column(
                MergeOperation.OPERATION_COLUMN, ConnectorComputeVariantType.INSTANCE.toCatalogDataType()));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> BindExpression.validateAndNormalizeMergeOutput(columns, source, new CTEContext()));
        Assertions.assertTrue(exception.getMessage().contains("input column 'operation'"), exception.getMessage());
    }
}
