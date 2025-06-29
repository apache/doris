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

package org.apache.doris.datasource.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Days;
import org.apache.iceberg.transforms.Hours;
import org.apache.iceberg.transforms.Months;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;

public class IcebergExternalTableTest {

    private org.apache.iceberg.Table icebergTable;
    private PartitionSpec spec;
    private PartitionField field;
    private Schema schema;
    private IcebergExternalCatalog mockCatalog;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        icebergTable = Mockito.mock(org.apache.iceberg.Table.class);
        spec = Mockito.mock(PartitionSpec.class);
        field = Mockito.mock(PartitionField.class);
        schema = Mockito.mock(Schema.class);
        mockCatalog = Mockito.mock(IcebergExternalCatalog.class);
    }

    @Test
    public void testIsSupportedPartitionTable() {
        IcebergExternalDatabase database = new IcebergExternalDatabase(mockCatalog, 1L, "2", "2");
        IcebergExternalTable table = new IcebergExternalTable(1, "1", "1", mockCatalog, database);

        // Create a spy to be able to mock the getIcebergTable method and the makeSureInitialized method
        IcebergExternalTable spyTable = Mockito.spy(table);
        Mockito.doReturn(icebergTable).when(spyTable).getIcebergTable();
        // Simulate the makeSureInitialized method as a no-op to avoid calling the parent class implementation
        Mockito.doNothing().when(spyTable).makeSureInitialized();

        Map<Integer, PartitionSpec> specs = Maps.newHashMap();

        // Test null
        specs.put(0, null);
        Mockito.when(icebergTable.specs()).thenReturn(specs);

        Assertions.assertFalse(spyTable.isValidRelatedTableCached());
        Assertions.assertFalse(spyTable.isValidRelatedTable());

        Mockito.verify(icebergTable, Mockito.times(1)).specs();
        Assertions.assertTrue(spyTable.isValidRelatedTableCached());
        Assertions.assertFalse(spyTable.validRelatedTableCache());

        // Test spec fields are empty.
        specs.put(0, spec);
        spyTable.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(spyTable.isValidRelatedTableCached());

        Mockito.when(icebergTable.specs()).thenReturn(specs);
        List<PartitionField> fields = Lists.newArrayList();
        Mockito.when(spec.fields()).thenReturn(fields);

        Assertions.assertFalse(spyTable.isValidRelatedTable());
        Mockito.verify(spec, Mockito.times(1)).fields();
        Assertions.assertTrue(spyTable.isValidRelatedTableCached());
        Assertions.assertFalse(spyTable.validRelatedTableCache());

        // Test spec fields are more than 1.
        specs.put(0, spec);
        spyTable.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(spyTable.isValidRelatedTableCached());

        Mockito.when(icebergTable.specs()).thenReturn(specs);
        fields.add(null);
        fields.add(null);
        Mockito.when(spec.fields()).thenReturn(fields);

        Assertions.assertFalse(spyTable.isValidRelatedTable());
        Mockito.verify(spec, Mockito.times(2)).fields();
        Assertions.assertTrue(spyTable.isValidRelatedTableCached());
        Assertions.assertFalse(spyTable.validRelatedTableCache());
        fields.clear();

        // Test true
        fields.add(field);
        spyTable.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(spyTable.isValidRelatedTableCached());

        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(schema.findColumnName(ArgumentMatchers.anyInt())).thenReturn("col1");
        Mockito.when(field.transform()).thenReturn(new Hours());
        Mockito.when(field.sourceId()).thenReturn(1);

        Assertions.assertTrue(spyTable.isValidRelatedTable());
        Assertions.assertTrue(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.validRelatedTableCache());
        Mockito.verify(schema, Mockito.times(1)).findColumnName(ArgumentMatchers.anyInt());

        Mockito.when(field.transform()).thenReturn(new Days());
        Mockito.when(field.sourceId()).thenReturn(1);
        spyTable.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.isValidRelatedTable());

        Mockito.when(field.transform()).thenReturn(new Months());
        Mockito.when(field.sourceId()).thenReturn(1);
        spyTable.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.isValidRelatedTable());
        Assertions.assertTrue(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.validRelatedTableCache());
    }
}
