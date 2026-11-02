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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Default;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.List;

public class ConnectorWriteSchemaUtilsTest extends ParserTestBase {

    private static final List<String> TARGET_NAME_PARTS = ImmutableList.of("catalog", "database", "test_table");

    @Test
    public void connectorHexDefaultParsesIdenticallyInBothBackslashModes() {
        Column column = new Column("path", ScalarType.createStringType());
        column.setConnectorDefaultValueSql("UNHEX('433A5C6E6577')");

        long originalSqlMode = ConnectContext.get().getSessionVariable().getSqlMode();
        try {
            ConnectContext.get().getSessionVariable().setSqlMode(0L);
            Expression defaultMode = ConnectorWriteSchemaUtils.resolveDefault(column);
            ConnectContext.get().getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
            Expression noBackslashEscapes = ConnectorWriteSchemaUtils.resolveDefault(column);

            Assertions.assertEquals(defaultMode, noBackslashEscapes);
            Assertions.assertEquals("unhex('433a5c6e6577')", defaultMode.toSql().toLowerCase());
        } finally {
            ConnectContext.get().getSessionVariable().setSqlMode(originalSqlMode);
        }
    }

    @Test
    public void connectorNonFiniteFloatingDefaultsParseAsTypedExpressions() {
        Column column = new Column("value", ScalarType.createStringType());
        for (String defaultSql : new String[] {
                "CAST('NaN' AS FLOAT)",
                "CAST('Infinity' AS DOUBLE)",
                "CAST('-Infinity' AS DOUBLE)",
                "named_struct('float_value', CAST('NaN' AS FLOAT), "
                        + "'double_values', array(CAST('Infinity' AS DOUBLE), "
                        + "CAST('-Infinity' AS DOUBLE)))"}) {
            column.setConnectorDefaultValueSql(defaultSql);
            Expression expression = ConnectorWriteSchemaUtils.resolveDefault(column);
            Assertions.assertTrue(expression.anyMatch(candidate -> candidate instanceof Cast),
                    "non-finite default must remain an explicitly typed cast: " + defaultSql);
            Assertions.assertFalse(expression.anyMatch(candidate -> candidate instanceof UnboundSlot),
                    "non-finite default must not be parsed as an identifier: " + defaultSql);
        }
    }

    @Test
    public void defaultReferencesResolveAgainstPinnedWriteColumns() {
        // DEFAULT(column) in DML resolves the statement-pinned write default; a name absent from the pinned writer
        // schema is an error rather than a guess.
        ExternalTable target = targetTable();
        List<Column> pinned = ImmutableList.of(pinnedColumn("score", 1, "9"));

        Assertions.assertEquals(new TinyIntLiteral((byte) 9), ConnectorWriteSchemaUtils.resolveDefaultReferences(
                new Default(new UnboundSlot("score")), pinned, target, ConnectContext.get(),
                TARGET_NAME_PARTS, null));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorWriteSchemaUtils.resolveDefaultReferences(
                        new Default(new UnboundSlot("missing")), pinned, target, ConnectContext.get(),
                        TARGET_NAME_PARTS, null));
        Assertions.assertTrue(exception.getMessage().contains("missing"), exception.getMessage());
    }

    @Test
    public void defaultOfBoundSlotDoesNotFallBackToReusedColumnName() {
        // A slot bound to a dropped and re-added column must not borrow the pinned default of the old field that
        // reused its name: a bound slot resolves DEFAULT by field id only.
        ExternalTable target = targetTable();
        Column pinnedField = pinnedColumn("reused_name", 7, "9");
        List<Column> pinned = ImmutableList.of(pinnedField);
        Column replacement = new Column("reused_name", ScalarType.createType(PrimitiveType.INT));
        replacement.setUniqueId(8);

        Assertions.assertEquals(new TinyIntLiteral((byte) 9), ConnectorWriteSchemaUtils.resolveDefaultReferences(
                new Default(boundSlot(target, pinnedField)), pinned, target, ConnectContext.get(),
                TARGET_NAME_PARTS, null));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorWriteSchemaUtils.resolveDefaultReferences(
                        new Default(boundSlot(target, replacement)), pinned, target, ConnectContext.get(),
                        TARGET_NAME_PARTS, null));
        Assertions.assertTrue(exception.getMessage().contains("Cannot find column information for DEFAULT"),
                exception.getMessage());
    }

    @Test
    public void connectorScalarDefaultsCastLosslesslyToTheirWriteColumnTypes() {
        // Connector write-default SQL is untyped text; binding casts it to the write column type, which must keep
        // the whole value: BIGINT beyond INT range, decimal scale, microseconds and dates.
        Assertions.assertEquals(new BigIntLiteral(4_900_000_001L),
                bindDefault(Type.BIGINT, "4900000001"));
        Assertions.assertEquals(new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(20, 4),
                new BigDecimal("98765.4321")), bindDefault(ScalarType.createDecimalV3Type(20, 4), "98765.4321"));
        Assertions.assertEquals(new DoubleLiteral(456.75), bindDefault(Type.DOUBLE, "456.75"));
        Assertions.assertEquals(new FloatLiteral(13.5F), bindDefault(Type.FLOAT, "13.5"));
        Assertions.assertEquals(BooleanLiteral.FALSE, bindDefault(Type.BOOLEAN, "false"));
        Assertions.assertEquals("2025-01-18",
                ((Literal) bindDefault(ScalarType.createDateV2Type(), "'2025-01-18'")).getStringValue());
        Assertions.assertEquals("2025-01-18 01:02:03.654321", ((Literal) bindDefault(
                ScalarType.createDatetimeV2Type(6), "'2025-01-18 01:02:03.654321'")).getStringValue());
    }

    /** Parse, cast and fold a connector default the way binding fills an omitted column. */
    private static Expression bindDefault(Type columnType, String defaultSql) {
        Column column = new Column("value", columnType);
        column.setConnectorDefaultValueSql(defaultSql);
        DataType targetType = DataType.fromCatalogType(columnType);
        Expression bound = FoldConstantRuleOnFE.evaluateWithoutContext(TypeCoercionUtils.castIfNotSameType(
                ConnectorWriteSchemaUtils.resolveDefault(column), targetType));
        Assertions.assertTrue(bound instanceof Literal, defaultSql + " -> " + bound);
        Assertions.assertEquals(targetType, bound.getDataType(), defaultSql);
        return bound;
    }

    private static Column pinnedColumn(String name, int uniqueId, String defaultSql) {
        Column column = new Column(name, ScalarType.createType(PrimitiveType.INT));
        column.setUniqueId(uniqueId);
        column.setConnectorDefaultValueSql(defaultSql);
        return column;
    }

    private static ExternalTable targetTable() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getId()).thenReturn(9L);
        return table;
    }

    private static SlotReference boundSlot(ExternalTable table, Column column) {
        return SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table, column, ImmutableList.of());
    }
}
