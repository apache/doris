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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TableName;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LancePreparedSearchTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPrepareAndRepeatedBindingUseFreshVectorAndSnapshot(boolean useIndex) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        LanceExternalTable table = Mockito.mock(LanceExternalTable.class);
        AtomicLong version = new AtomicLong(42);
        Schema schema = new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Int(64, true)),
                new Field("embedding", org.apache.arrow.vector.types.pojo.FieldType.nullable(
                        new ArrowType.FixedSizeList(2)), Collections.singletonList(
                                Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))));
        Mockito.when(table.loadMetadataForSearch()).thenAnswer(invocation -> LanceTableMetadata.withIndexSegments(
                "s3://bucket/items.lance", version.get(), schema, Collections.emptyList(),
                ImmutableMap.of("id", 0, "embedding", 1), Collections.emptyList(), Collections.emptyMap()));
        Mockito.when(table.loadMetadata()).thenAnswer(invocation -> table.loadMetadataForSearch());
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            Pair<LogicalPlan, StatementContext> parsed = new NereidsParser().parseMultiple(
                    "select id, _distance from vector_search('table'='catalog.db.items', "
                            + "'column'='embedding', 'use_index'='" + useIndex + "', "
                            + "'query_vector'=?, 'top_k'=?, 'offset'=?, 'filter'=?)")
                    .get(0);
            List<Placeholder> parameters = parsed.second.getPlaceholders();
            Assertions.assertEquals(4, parameters.size());
            StatementContext prepare = MemoTestUtils.createStatementContext(context, "");
            prepare.setPrepareStage(true);
            VectorSearchTableValuedFunction prepared = analyze(parsed.first, prepare);
            Assertions.assertEquals(3, prepared.getTableColumns().size());
            Assertions.assertFalse(prepared.getSearchRequest().getSearchQuery().getVectorSearch().isSetQueryVector());

            LogicalPlan staticVector = new NereidsParser().parseMultiple(
                    "select * from vector_search('table'='catalog.db.items', 'column'='embedding', "
                            + "'query_vector'='[1,2]', 'top_k'=?)").get(0).first;
            Assertions.assertTrue(analyze(staticVector, prepare).getSearchRequest()
                    .getSearchQuery().getVectorSearch().isSetQueryVector());

            for (int i = 0; i < 2; i++) {
                version.set(42 + i);
                StatementContext execute = MemoTestUtils.createStatementContext(context, "");
                execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                        new StringLiteral(i == 0 ? "[1,2]" : "[3,4]"));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(3 + i));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(2).getPlaceholderId(), new IntegerLiteral(i));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(3).getPlaceholderId(),
                        new StringLiteral("id > " + i));
                VectorSearchTableValuedFunction function = analyze(parsed.first, execute);
                Assertions.assertEquals(42 + i, function.getMetadata().getVersion());
                Assertions.assertEquals(3 + i, function.getTopK());
                Assertions.assertEquals(i, function.getOffset());
                Assertions.assertEquals("id > " + i, new String(
                        function.getSearchRequest().getSearchFilter().getPayload(), StandardCharsets.UTF_8));
                byte[] values = function.getSearchRequest().getSearchQuery().getVectorSearch().getQueryVector().getValues();
                Assertions.assertEquals(i == 0 ? 1.0f : 3.0f,
                        ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN).getFloat());
                for (String invalidVector : new String[] {"[1]", "not-json"}) {
                    execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                            new StringLiteral(invalidVector));
                    Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                            () -> analyze(parsed.first, execute));
                }
                execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(), NullLiteral.INSTANCE);
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                        new StringLiteral("[1,2]"));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(0));
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(3));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(2).getPlaceholderId(), new IntegerLiteral(-1));
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
                execute.getIdToPlaceholderRealExpr().remove(parameters.get(0).getPlaceholderId());
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
            }
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private VectorSearchTableValuedFunction analyze(LogicalPlan plan, StatementContext statement) {
        CascadesContext cascades = CascadesContext.initContext(statement, plan, PhysicalProperties.ANY);
        cascades.newAnalyzer().analyze();
        LogicalTVFRelation relation = cascades.getRewritePlan().<LogicalTVFRelation>collectToList(
                LogicalTVFRelation.class::isInstance).get(0);
        return (VectorSearchTableValuedFunction) relation.getFunction().getCatalogFunction();
    }
}
