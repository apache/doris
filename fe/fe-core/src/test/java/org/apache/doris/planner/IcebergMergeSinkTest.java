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
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergRewritableDeleteFileSet;

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

public class IcebergMergeSinkTest {

    @Test
    public void testBindDataSinkIncludesRowLineageSchemaAndRewritableDeleteFileSetsForV3() throws Exception {
        IcebergMergeSink sink = new IcebergMergeSink(mockIcebergExternalTable(3), new DeleteCommandContext());
        sink.setRewritableDeleteFileSets(Collections.singletonList(buildDeleteFileSet()));

        sink.bindDataSink(Optional.empty());

        TIcebergMergeSink thriftSink = sink.tDataSink.getIcebergMergeSink();
        Assertions.assertEquals(3, thriftSink.getFormatVersion());
        Assertions.assertTrue(thriftSink.getSchemaJson().contains(IcebergUtils.ICEBERG_ROW_ID_COL));
        Assertions.assertTrue(thriftSink.getSchemaJson().contains(
                IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL));
        Assertions.assertEquals(1, thriftSink.getRewritableDeleteFileSetsSize());
    }

    @Test
    public void testBindDataSinkSkipsRewritableDeleteFileSetsAndRowLineageSchemaForV2() throws Exception {
        IcebergMergeSink sink = new IcebergMergeSink(mockIcebergExternalTable(2), new DeleteCommandContext());
        sink.setRewritableDeleteFileSets(Collections.singletonList(buildDeleteFileSet()));

        sink.bindDataSink(Optional.empty());

        TIcebergMergeSink thriftSink = sink.tDataSink.getIcebergMergeSink();
        Assertions.assertEquals(2, thriftSink.getFormatVersion());
        Assertions.assertFalse(thriftSink.isSetRewritableDeleteFileSets());
        Assertions.assertFalse(thriftSink.getSchemaJson().contains(IcebergUtils.ICEBERG_ROW_ID_COL));
        Assertions.assertFalse(thriftSink.getSchemaJson().contains(
                IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL));
    }

    private static TIcebergRewritableDeleteFileSet buildDeleteFileSet() {
        TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
        deleteFileDesc.setPath("file:///tmp/delete.puffin");
        TIcebergRewritableDeleteFileSet deleteFileSet = new TIcebergRewritableDeleteFileSet();
        deleteFileSet.setReferencedDataFilePath("file:///tmp/data.parquet");
        deleteFileSet.setDeleteFiles(Collections.singletonList(deleteFileDesc));
        return deleteFileSet;
    }

    private static IcebergExternalTable mockIcebergExternalTable(int formatVersion) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
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
