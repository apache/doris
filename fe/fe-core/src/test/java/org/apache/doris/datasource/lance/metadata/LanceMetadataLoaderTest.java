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

package org.apache.doris.datasource.lance.metadata;

import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalTable;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Dataset;
import org.mockito.Mockito;

import java.util.Collections;

public class LanceMetadataLoaderTest {
    @Test
    public void testSnapshotReadDoesNotDiscoverIndexesOrConvertLanceSchema() {
        Dataset dataset = emptyDataset();
        LanceTableMetadata metadata = LanceMetadataLoader.read(dataset,
                new LanceTableAccess("table.lance", Collections.emptyMap()),
                LanceMetadataLoader.MetadataScope.BASIC);
        Assertions.assertEquals(7, metadata.getVersion());
        Assertions.assertTrue(metadata.getIndexes().isEmpty());
        Assertions.assertEquals(LanceTableMetadata.IndexMetadataState.NOT_LOADED, metadata.getIndexMetadataState());
        Mockito.verify(dataset, Mockito.never()).getLanceSchema();
        Mockito.verify(dataset, Mockito.never()).listIndexes();
        Mockito.verify(dataset, Mockito.never()).getIndexes();
        Mockito.verify(dataset, Mockito.never()).close();
    }

    @Test
    public void testKnownSchemaConversionFailureIsExplicitInMetadata() {
        Dataset dataset = emptyDataset();
        Mockito.when(dataset.getLanceSchema()).thenThrow(new IllegalArgumentException("ArrowSchema conversion error"));
        Mockito.when(dataset.listIndexes()).thenReturn(Collections.emptyList());
        LanceTableMetadata metadata = LanceMetadataLoader.read(dataset,
                new LanceTableAccess("table.lance", Collections.emptyMap()),
                LanceMetadataLoader.MetadataScope.WITH_INDEXES);
        Assertions.assertFalse(metadata.getLanceFieldId("id").isPresent());
        // A schema fallback must still perform index discovery and validation.
        Mockito.verify(dataset).listIndexes();
        Assertions.assertEquals(LanceTableMetadata.IndexMetadataState.FIELD_IDS_UNAVAILABLE,
                metadata.getIndexMetadataState());
        Mockito.doThrow(new IllegalArgumentException("another failure")).when(dataset).getLanceSchema();
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceMetadataLoader.read(
                dataset, new LanceTableAccess("table.lance", Collections.emptyMap()), LanceMetadataLoader.MetadataScope.WITH_INDEXES));
    }

    @Test
    public void testSchemaFallbackDoesNotHideInvalidIndexMetadata() {
        Dataset dataset = emptyDataset();
        Mockito.when(dataset.getLanceSchema()).thenThrow(new IllegalArgumentException("ArrowSchema conversion error"));
        Mockito.when(dataset.listIndexes()).thenReturn(null);
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceMetadataLoader.read(dataset, new LanceTableAccess("table.lance", Collections.emptyMap()),
                        LanceMetadataLoader.MetadataScope.WITH_INDEXES));
        Assertions.assertTrue(failure.getMessage().contains("index names must not be null"));
    }

    @Test
    public void testSchemaInitializationUsesOnlySchemaRead() {
        Schema schema = new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        LanceExternalDatabase database = Mockito.mock(LanceExternalDatabase.class);
        Mockito.when(database.getRemoteName()).thenReturn("db");
        Mockito.when(catalog.loadTableSchema("db", "tbl")).thenReturn(schema);
        LanceExternalTable table = new LanceExternalTable(1, "tbl", "tbl", catalog, database);
        Assertions.assertEquals("id", table.initSchema().get().getSchema().get(0).getName());
        Mockito.verify(catalog).loadTableSchema("db", "tbl");
        Mockito.verify(catalog, Mockito.never()).loadTableMetadata(Mockito.anyString(), Mockito.anyString());
    }

    private static Dataset emptyDataset() {
        Dataset dataset = Mockito.mock(Dataset.class);
        Mockito.when(dataset.version()).thenReturn(7L);
        Mockito.when(dataset.getFragments()).thenReturn(Collections.emptyList());
        Mockito.when(dataset.getSchema()).thenReturn(new Schema(Collections.emptyList()));
        return dataset;
    }
}
