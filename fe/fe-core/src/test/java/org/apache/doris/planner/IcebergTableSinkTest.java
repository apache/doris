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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class IcebergTableSinkTest {
    @Test
    public void testBindDataSinkRejectsVariantSchema() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.VariantType.get()));
        IcebergTableSink sink = new IcebergTableSink(mockIcebergExternalTable(schema));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> sink.bindDataSink(Optional.empty()));
        Assertions.assertTrue(exception.getMessage().contains(
                "Writing Iceberg VARIANT columns is not supported: payload"));
    }

    private static IcebergExternalTable mockIcebergExternalTable(Schema schema) {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "2");
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        properties.put(TableProperties.PARQUET_COMPRESSION, "snappy");
        properties.put(TableProperties.WRITE_DATA_LOCATION, "file:///tmp/iceberg_tbl/data");

        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.properties()).thenReturn(properties);
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
        Mockito.when(icebergTable.location()).thenReturn("file:///tmp/iceberg_tbl");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(icebergTable.name()).thenReturn("db.tbl");

        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(null);
        Mockito.when(catalogProperty.getStoragePropertiesMap()).thenReturn(Collections.emptyMap());

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);

        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.isView()).thenReturn(false);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getDbName()).thenReturn("db");
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getIcebergTable()).thenReturn(icebergTable);
        return table;
    }
}
