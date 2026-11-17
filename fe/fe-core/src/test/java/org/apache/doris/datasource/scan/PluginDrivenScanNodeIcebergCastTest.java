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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.iceberg.IcebergCatalogProperties;
import org.apache.doris.connector.iceberg.IcebergConnectorMetadata;
import org.apache.doris.connector.iceberg.IcebergPredicateConverter;
import org.apache.doris.connector.iceberg.IcebergTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Exercises planned CAST predicates through the engine's filter gate and actual Iceberg pruning. */
public class PluginDrivenScanNodeIcebergCastTest extends TestWithFeService {
    private static final Types.DecimalType DECIMAL = Types.DecimalType.of(38, 0);
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "wide", DECIMAL));

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("iceberg_cast_test");
        createTables("create table iceberg_cast_test.cast_predicates (id int, wide decimal(38, 0))"
                + " distributed by hash(id) properties('replication_num' = '1')");
    }

    @ParameterizedTest
    @CsvSource({"9007199254740993, 9007199254740992, false",
            "9007199254740993, 9007199254740992, true",
            "-9007199254740993, -9007199254740992, false",
            "-9007199254740993, -9007199254740992, true"})
    public void roundedDecimalEqualityKeepsMatchingRowsAndFiles(String rowText, String literal, boolean reversed)
            throws Exception {
        BigDecimal row = new BigDecimal(rowText);
        Assertions.assertEquals(Double.parseDouble(literal), row.doubleValue());
        String cast = "cast(wide as double)";
        String constant = "cast(" + literal + " as double)";
        String predicate = reversed ? constant + " = " + cast : cast + " = " + constant;
        List<Expr> conjuncts = planConjuncts(predicate);
        assertDecimalToDoubleColumnCast(conjuncts);

        ConnectorScanRequest request = scanRequest(conjuncts);

        assertMatchingRowAndFileSurvive(request, row);
        Assertions.assertTrue(request.getFilter().isEmpty());
        Assertions.assertEquals(-1L, request.getLimit());
    }

    @Test
    public void disjunctionContainingCastStaysWhole() throws Exception {
        BigDecimal row = new BigDecimal("9007199254740993");
        Assertions.assertEquals(9007199254740992d, row.doubleValue());
        assertCastPredicateStaysLocal(
                "cast(wide as double) = cast(9007199254740992 as double) or id = 2", row);
    }

    @Test
    public void negatedCastPredicateStaysLocal() throws Exception {
        BigDecimal row = new BigDecimal("9007199254740995");
        Assertions.assertNotEquals(9007199254740992d, row.doubleValue());
        assertCastPredicateStaysLocal(
                "not (cast(wide as double) = cast(9007199254740992 as double))", row);
    }

    private void assertCastPredicateStaysLocal(String predicate, BigDecimal matchingRow) throws Exception {
        List<Expr> conjuncts = planConjuncts(predicate);
        assertDecimalToDoubleColumnCast(conjuncts);
        ConnectorScanRequest request = scanRequest(conjuncts);
        Assertions.assertTrue(request.getFilter().isEmpty(), predicate);
        Assertions.assertEquals(-1L, request.getLimit());
        assertMatchingRowAndFileSurvive(request, matchingRow);
    }

    @Test
    public void independentAndConjunctStillPrunesWithoutPushingLimit() throws Exception {
        List<Expr> conjuncts = planConjuncts(
                "cast(wide as double) = cast(9007199254740992 as double) and id = 1");
        Assertions.assertEquals(2, conjuncts.size());
        assertDecimalToDoubleColumnCast(conjuncts);

        ConnectorScanRequest request = scanRequest(conjuncts);

        ConnectorComparison pushed = Assertions.assertInstanceOf(ConnectorComparison.class,
                request.getFilter().orElseThrow());
        Assertions.assertEquals("id", ((ConnectorColumnRef) pushed.getLeft()).getColumnName());
        Assertions.assertEquals(-1L, request.getLimit());
        assertMatchingRowAndFileSurvive(request, new BigDecimal("9007199254740993"));
    }

    @Test
    public void plainPredicatesRemainPushable() throws Exception {
        List<Expr> conjuncts = planConjuncts("id = 1");
        Assertions.assertFalse(conjuncts.stream().anyMatch(PluginDrivenScanNode::containsCastExpr));

        ConnectorScanRequest request = scanRequest(conjuncts);

        Assertions.assertTrue(request.getFilter().isPresent());
        Assertions.assertEquals(1L, request.getLimit());
        assertMatchingRowAndFileSurvive(request, new BigDecimal("9007199254740993"));
    }

    private static void assertDecimalToDoubleColumnCast(List<Expr> conjuncts) {
        List<Expr> casts = new ArrayList<>();
        for (Expr conjunct : conjuncts) {
            conjunct.collect(node -> node instanceof CastExpr && node.getType().equals(Type.DOUBLE)
                    && node.getChild(0) instanceof SlotRef && node.getChild(0).getType().isDecimalV3(), casts);
        }
        Assertions.assertFalse(casts.isEmpty(), "the planned column comparison must still contain its numeric CAST");
    }

    private List<Expr> planConjuncts(String predicate) throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext,
                "select * from iceberg_cast_test.cast_predicates where " + predicate);
        executor.execute();
        Assertions.assertNotEquals(org.apache.doris.qe.QueryState.MysqlStateType.ERR,
                connectContext.getState().getStateType(), connectContext.getState().getErrorMessage());
        return executor.planner().getScanNodes().get(0).getConjuncts();
    }

    private static ConnectorScanRequest scanRequest(List<Expr> conjuncts) {
        List<Expr> before = Expr.cloneList(conjuncts);
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(
                null, IcebergCatalogProperties.of(Collections.emptyMap()), null);
        Deencapsulation.setField(node, "cachedMetadata", metadata);
        Deencapsulation.setField(node, "conjuncts", conjuncts);
        Optional<ConnectorExpression> filter = Deencapsulation.invoke(node, "buildRemainingFilter");
        List<Integer> indexMapping = Deencapsulation.getField(node, "filteredToOriginalIndex");
        Assertions.assertEquals(before, conjuncts, "original predicates must remain for Doris to evaluate");
        return ConnectorScanRequest.builder(new IcebergTableHandle("iceberg_cast_test", "cast_predicates"),
                Collections.emptyList()).filter(filter)
                .limit(PluginDrivenScanNode.effectiveSourceLimit(1L, indexMapping != null)).build();
    }

    private static void assertMatchingRowAndFileSurvive(ConnectorScanRequest request, BigDecimal value) {
        Expression filter = Expressions.alwaysTrue();
        for (Expression predicate : request.getFilter().map(expression ->
                new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC).convert(expression))
                .orElse(Collections.emptyList())) {
            filter = Expressions.and(filter, predicate);
        }
        Map<Integer, ByteBuffer> bounds = Map.of(
                1, Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                2, Conversions.toByteBuffer(DECIMAL, value));
        Metrics metrics = new Metrics(1L, null, Map.of(1, 1L, 2, 1L),
                Map.of(1, 0L, 2, 0L), null, bounds, bounds);
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("data/decimal-double.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L).withMetrics(metrics).build();
        Object[] values = {1, value};
        StructLike row = new StructLike() {
            @Override
            public int size() {
                return values.length;
            }

            @Override
            public <T> T get(int position, Class<T> javaClass) {
                return javaClass.cast(values[position]);
            }

            @Override
            public <T> void set(int position, T newValue) {
                throw new UnsupportedOperationException();
            }
        };
        Expression pruningFilter = filter;
        Assertions.assertAll(
                () -> Assertions.assertTrue(new Evaluator(SCHEMA.asStruct(), pruningFilter).eval(row),
                        "a matching row must survive pruning"),
                () -> Assertions.assertTrue(new InclusiveMetricsEvaluator(SCHEMA, pruningFilter).eval(file),
                        "a file containing a matching row must survive pruning"));
    }
}
