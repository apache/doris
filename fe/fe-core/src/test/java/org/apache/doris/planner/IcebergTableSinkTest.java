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

package org.apache.doris.planner;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.thrift.TIcebergTableSink;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class IcebergTableSinkTest {
    @Test
    public void testBindUsesPinnedIcebergTableMetadata() throws Exception {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(null);
        Mockito.when(catalogProperty.getStoragePropertiesMap()).thenReturn(Collections.emptyMap());

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(targetTable.isView()).thenReturn(false);
        Mockito.when(targetTable.getCatalog()).thenReturn(catalog);
        Mockito.when(targetTable.getDbName()).thenReturn("db");
        Mockito.when(targetTable.getName()).thenReturn("table");

        Schema pinnedSchema = new Schema(1,
                Types.NestedField.required(1, "pinned_id", Types.IntegerType.get()));
        Table pinnedTable = mockTable(pinnedSchema);
        Schema liveSchema = new Schema(2,
                Types.NestedField.required(2, "live_id", Types.IntegerType.get()));
        Table liveTable = mockTable(liveSchema);
        Mockito.when(targetTable.getIcebergTable()).thenReturn(liveTable);

        IcebergTableSink sink = new IcebergTableSink(targetTable, pinnedTable);
        sink.bindDataSink(Optional.empty());

        TIcebergTableSink thriftSink = sink.tDataSink.getIcebergTableSink();
        Assertions.assertTrue(thriftSink.getSchemaJson().contains("pinned_id"));
        Assertions.assertFalse(thriftSink.getSchemaJson().contains("live_id"));
    }

    @Test
    public void testRewriteKeepsPinnedFormatVersionForRowLineageSchema() throws Exception {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(null);
        Mockito.when(catalogProperty.getStoragePropertiesMap()).thenReturn(Collections.emptyMap());

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(targetTable.isView()).thenReturn(false);
        Mockito.when(targetTable.getCatalog()).thenReturn(catalog);
        Mockito.when(targetTable.getDbName()).thenReturn("db");
        Mockito.when(targetTable.getName()).thenReturn("table");

        Schema schema = new Schema(1,
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table pinnedV3Table = mockTable(schema,
                Collections.singletonMap(TableProperties.FORMAT_VERSION, "3"));
        Table refreshedV2Table = mockTable(schema,
                Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));
        Mockito.when(targetTable.getIcebergTable()).thenReturn(pinnedV3Table, refreshedV2Table);

        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setRewriting(true);
        IcebergTableSink sink = new IcebergTableSink(targetTable);
        sink.bindDataSink(Optional.of(insertContext));

        String schemaJson = sink.tDataSink.getIcebergTableSink().getSchemaJson();
        Assertions.assertTrue(schemaJson.contains("_row_id"));
        Assertions.assertTrue(schemaJson.contains("_last_updated_sequence_number"));
        Mockito.verify(targetTable, Mockito.times(1)).getIcebergTable();
    }

    private static Table mockTable(Schema schema) {
        return mockTable(schema, Collections.emptyMap());
    }

    private static Table mockTable(Schema schema, Map<String, String> properties) {
        Table table = Mockito.mock(Table.class);
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        Mockito.when(table.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
        Mockito.when(table.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(table.properties()).thenReturn(properties);
        Mockito.when(table.location()).thenReturn("file:///tmp/iceberg-table");
        Mockito.when(table.name()).thenReturn("table");
        return table;
    }
}
