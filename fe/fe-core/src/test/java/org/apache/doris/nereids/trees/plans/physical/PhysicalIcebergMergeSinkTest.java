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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext;
import org.apache.doris.nereids.properties.DistributionSpecMerge;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PhysicalIcebergMergeSinkTest {
    @Test
    public void nestedPartitionRoutesThroughRootIdAfterRename() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "payload",
                Types.StructType.of(Types.NestedField.optional(2, "part", Types.IntegerType.get()))));
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("payload.part", 8).build();
        Column root = Mockito.mock(Column.class);
        Column leaf = new Column("renamed_part", Type.INT);
        leaf.setUniqueId(2);
        Mockito.when(root.getUniqueId()).thenReturn(1);
        Mockito.when(root.getChildren()).thenReturn(new ArrayList<>(Collections.singletonList(leaf)));
        PhysicalIcebergMergeSink<Plan> sink = new PhysicalIcebergMergeSink<>(
                Mockito.mock(IcebergExternalDatabase.class), Mockito.mock(IcebergExternalTable.class),
                Mockito.mock(Table.class), Collections.singletonList(root), Collections.emptyList(),
                new DeleteCommandContext(), true, false, Optional.empty(),
                Mockito.mock(LogicalProperties.class),
                new PhysicalEmptyRelation(new RelationId(1), Collections.emptyList(), null));
        Method build = PhysicalIcebergMergeSink.class.getDeclaredMethod("buildInsertPartitionFields",
                List.class, PartitionSpec.class, Schema.class, Map.class);
        build.setAccessible(true);
        ExprId rootExprId = new ExprId(42);
        List<DistributionSpecMerge.IcebergPartitionField> fields = new ArrayList<>();

        build.invoke(sink, fields, spec, schema, Collections.singletonMap(1, rootExprId));

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(rootExprId, fields.get(0).getSourceExprId());
        Assertions.assertEquals(2, fields.get(0).getSourceId());
        Assertions.assertEquals(Collections.singletonList(0), fields.get(0).getSourceFieldPath());
        Assertions.assertEquals(8, fields.get(0).getParam());

        // A missing stable root ID must disable field-based routing to prevent a wrong partition.
        fields.clear();
        Mockito.when(root.getUniqueId()).thenReturn(-1);
        build.invoke(sink, fields, spec, schema, Collections.singletonMap(1, rootExprId));
        Assertions.assertTrue(fields.isEmpty());
    }

    @Test
    public void v3LineageSlotsDoNotDisablePartitionRouting() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "payload", Types.StructType.of(
                        Types.NestedField.optional(2, "part", Types.IntegerType.get()))),
                Types.NestedField.optional(3, "id", Types.IntegerType.get()));
        List<Column> columns = IcebergWriteSchemaContext.forSchema(schema, 3, true, true).getMergeColumns();
        List<NamedExpression> output = new ArrayList<>();
        for (Column column : columns) {
            output.add(new SlotReference(column.getName(), DataType.fromCatalogType(column.getType())));
        }
        output.add(new SlotReference(IcebergMergeOperation.OPERATION_COLUMN, IntegerType.INSTANCE));
        PhysicalEmptyRelation child = new PhysicalEmptyRelation(new RelationId(2), output, null);
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().enableIcebergMergePartitioning = true;
        context.setThreadLocalInfo();
        try {
            for (PartitionSpec spec : new PartitionSpec[] {
                    PartitionSpec.builderFor(schema).bucket("id", 8).build(),
                    PartitionSpec.builderFor(schema).bucket("payload.part", 8).build(),
                    PartitionSpec.builderFor(schema).identity("payload.part").build()}) {
                Table table = Mockito.mock(Table.class);
                Mockito.when(table.schema()).thenReturn(schema);
                Mockito.when(table.spec()).thenReturn(spec);
                PhysicalIcebergMergeSink<PhysicalEmptyRelation> sink = new PhysicalIcebergMergeSink<>(
                        Mockito.mock(IcebergExternalDatabase.class), Mockito.mock(IcebergExternalTable.class),
                        table, columns, output, new DeleteCommandContext(), true, false,
                        Optional.empty(), null, child);
                DistributionSpecMerge distribution = (DistributionSpecMerge)
                        sink.getRequirePhysicalProperties().getDistributionSpec();
                Assertions.assertFalse(distribution.isInsertRandom());
                Assertions.assertEquals(1, distribution.getInsertPartitionFields().size());
                Assertions.assertEquals(spec.fields().get(0).sourceId(),
                        distribution.getInsertPartitionFields().get(0).getSourceId());
            }
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }
}
