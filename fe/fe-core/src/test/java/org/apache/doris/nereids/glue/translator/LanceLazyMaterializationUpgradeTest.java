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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.datasource.lance.source.LanceScanNode;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.FullTextSearch;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeTVFScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.FullTextSearchTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class LanceLazyMaterializationUpgradeTest {
    @Test
    public void testVectorSearchLazyNullUpgradeFence() throws Exception {
        assertUpgradeFence(true);
    }

    @Test
    public void testFullTextSearchLazyNullUpgradeFence() throws Exception {
        assertUpgradeFence(false);
    }

    @Test
    public void testMultiVectorSearchFencesOldBackendWithoutVectorProjection() throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                    () -> translate(true, false, true, true));
            Assertions.assertTrue(error.toString().contains("smooth upgrade source"));
            translate(true, false, false, true);
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    private void assertUpgradeFence(boolean vector) throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> translate(vector, true, true));
            Assertions.assertTrue(exception.toString().contains("smooth upgrade source"), exception.toString());
            translate(vector, true, false);
            // An unreferenced nested Null field must not fence an ordinary lazy projection.
            translate(vector, false, true);
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    private void translate(boolean vector, boolean lazyNull, boolean mixedVersion) throws Exception {
        translate(vector, lazyNull, mixedVersion, false);
    }

    private void translate(boolean vector, boolean lazyNull, boolean mixedVersion, boolean multiVector) throws Exception {
        String functionName = vector ? "vector_search" : "full_text_search";
        String scoreName = vector ? "_distance" : "_score";
        Field nestedNull = new Field("nested_null", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(Field.nullable("item", ArrowType.Null.INSTANCE)));
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance", 42,
                new Schema(Arrays.asList(nestedNull, Field.nullable("ordinary", ArrowType.Utf8.INSTANCE))),
                Collections.emptyList(), Collections.emptyMap());
        Column score = new Column(scoreName, Type.DOUBLE);
        Column payload = new Column("nested_null", ArrayType.create(Type.NULL, true));
        Column ordinary = new Column("ordinary", Type.STRING);
        Column rowIdColumn = new Column(Column.GLOBAL_ROWID_COL + functionName, Type.STRING);
        TableValuedFunctionIf catalogFunction = vector
                ? Mockito.mock(VectorSearchTableValuedFunction.class)
                : Mockito.mock(FullTextSearchTableValuedFunction.class);
        FunctionGenTable table = new FunctionGenTable(1, functionName, Table.TableType.TABLE_VALUED_FUNCTION,
                Arrays.asList(score, payload, ordinary), catalogFunction);
        TableValuedFunction function = vector ? Mockito.mock(VectorSearch.class) : Mockito.mock(FullTextSearch.class);
        Mockito.when(function.getName()).thenReturn(functionName);
        Mockito.when(function.getTable()).thenReturn(table);
        Mockito.when(function.getCatalogFunction()).thenReturn(catalogFunction);
        SlotReference scoreSlot = SlotReference.fromColumn(new ExprId(1), table, score, Collections.emptyList());
        // Retain the source column identity even when the output uses an alias.
        SlotReference lazySlot = SlotReference.fromColumn(new ExprId(2), table,
                lazyNull ? payload : ordinary, "payload_alias", Collections.emptyList());
        SlotReference rowId = SlotReference.fromColumn(new ExprId(3), table, rowIdColumn, Collections.emptyList());
        List<Slot> output = Arrays.asList(scoreSlot, lazySlot);
        PhysicalTVFRelation relation = new PhysicalTVFRelation(new RelationId(1), function,
                Collections.singletonList(scoreSlot), new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT));
        PhysicalLazyMaterializeTVFScan lazyScan = new PhysicalLazyMaterializeTVFScan(
                relation, rowId, Collections.singletonList(lazySlot));
        Assertions.assertFalse(lazyScan.getOutput().contains(lazySlot));

        Backend current = Mockito.mock(Backend.class);
        Backend old = Mockito.mock(Backend.class);
        Mockito.when(old.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(old.getId()).thenReturn(102L);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.getBackends()).thenReturn(mixedVersion
                ? Arrays.asList(current, old) : Collections.singletonList(current));
        LanceExternalTable source = Mockito.mock(LanceExternalTable.class);
        AtomicReference<LanceScanNode> translated = new AtomicReference<>();
        Mockito.when(catalogFunction.getScanNode(Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(call -> {
            TExternalSearchQuery query = vector
                    ? TExternalSearchQuery.vector_search(new TVectorSearchParams().setColumn("vector"))
                    : TExternalSearchQuery.full_text_search(new TFullTextSearchParams().setColumn("text"));
            LanceScanNode node = LanceScanNode.forExternalSearch(call.getArgument(0, PlanNodeId.class),
                    call.getArgument(1, TupleDescriptor.class), source, metadata, 0,
                    new TExternalSearchRequest().setSchemaVersion(multiVector ? 2 : 1).setSearchQuery(query),
                    call.getArgument(2, SessionVariable.class));
            java.lang.reflect.Field backendPolicy = ExternalScanNode.class.getDeclaredField("backendPolicy");
            backendPolicy.setAccessible(true);
            backendPolicy.set(node, policy);
            translated.set(node);
            return node;
        });
        PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        lazyScan.accept(new PhysicalPlanTranslator(translatorContext), translatorContext);
        Assertions.assertFalse(translated.get().getTupleDesc().getSlots().stream()
                .anyMatch(slot -> "nested_null".equals(slot.getColumn().getName())));
    }
}
