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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Default;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class RewriteDefaultExpressionTest extends ExpressionRewriteTestHelper {

    private static final long TABLE_ID = 17L;
    private static final int FIELD_ID = 3;

    @BeforeEach
    public void setUp() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp(RewriteDefaultExpression.INSTANCE)));
    }

    @Test
    public void writeOnlyDefaultResolvesAgainstPinnedWriterSchema() {
        // UPDATE t ... WHERE score = DEFAULT(score): the scanned column carries no write-only default, the writer
        // schema pinned for the statement's target does.
        SlotReference score = scoreSlot(true);
        pinWriterColumn("9");

        Expression rewritten = executor.rewrite(new Default(score), context);

        Assertions.assertEquals(
                TypeCoercionUtils.castIfNotSameType(PARSER.parseExpression("9"), IntegerType.INSTANCE), rewritten);
    }

    @Test
    public void writeOnlyDefaultOutsideTheWriteTargetIsRejected() {
        // SELECT DEFAULT(score) FROM t writes nothing, so nothing carries the write-only default.
        SlotReference score = scoreSlot(true);

        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> executor.rewrite(new Default(score), context));

        Assertions.assertTrue(e.getMessage().contains(
                "DEFAULT(score) must reference a column of the table that this statement writes"), e.getMessage());
    }

    @Test
    public void readSchemaDefaultIsKeptWithoutWriteOnlyDefaults() {
        // A connector whose read schema carries its defaults (hive) resolves from the scanned column as before.
        SlotReference score = scoreSlot(false);
        pinWriterColumn("9");

        Expression rewritten = executor.rewrite(new Default(score), context);

        Assertions.assertEquals(new NullLiteral(IntegerType.INSTANCE), rewritten);
    }

    private SlotReference scoreSlot(boolean writeOnlyDefaults) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.hasWriteOnlyDefaults()).thenReturn(writeOnlyDefaults);
        Column readColumn = new Column("score", PrimitiveType.INT, true);
        readColumn.setUniqueId(FIELD_ID);
        return SlotReference.fromColumn(new ExprId(1), table, readColumn, ImmutableList.of());
    }

    private void pinWriterColumn(String defaultSql) {
        Column writeColumn = new Column("score", PrimitiveType.INT, true);
        writeColumn.setUniqueId(FIELD_ID);
        writeColumn.setConnectorDefaultValueSql(defaultSql);
        cascadesContext.getStatementContext().setConnectorWriteSchema(TABLE_ID, ImmutableList.of(writeColumn));
    }
}
