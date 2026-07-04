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
import org.apache.iceberg.transforms.Transform;
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
        Mockito.doReturn(mockTransform("hour")).when(field).transform();
        Mockito.when(field.sourceId()).thenReturn(1);

        Assertions.assertTrue(spyTable.isValidRelatedTable());
        Assertions.assertTrue(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.validRelatedTableCache());
        Mockito.verify(schema, Mockito.times(1)).findColumnName(ArgumentMatchers.anyInt());

        Mockito.doReturn(mockTransform("day")).when(field).transform();
        Mockito.when(field.sourceId()).thenReturn(1);
        spyTable.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.isValidRelatedTable());

        Mockito.doReturn(mockTransform("month")).when(field).transform();
        Mockito.when(field.sourceId()).thenReturn(1);
        spyTable.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.isValidRelatedTable());
        Assertions.assertTrue(spyTable.isValidRelatedTableCached());
        Assertions.assertTrue(spyTable.validRelatedTableCache());
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private IcebergExternalTable createSpyTable() {
        IcebergExternalDatabase db = new IcebergExternalDatabase(mockCatalog, 1L, "db", "db");
        IcebergExternalTable t = new IcebergExternalTable(1, "tbl", "tbl", mockCatalog, db);
        IcebergExternalTable spy = Mockito.spy(t);
        Mockito.doReturn(icebergTable).when(spy).getIcebergTable();
        Mockito.doNothing().when(spy).makeSureInitialized();
        return spy;
    }

    @Test
    public void testGetComment() {
        IcebergExternalTable spy = createSpyTable();
        Map<String, String> properties = Maps.newHashMap();
        properties.put("comment", "my-table-comment");
        Mockito.when(icebergTable.properties()).thenReturn(properties);

        Assertions.assertEquals("my-table-comment", spy.getComment());

        properties.put("comment", "comment with \"quote\"");
        Assertions.assertEquals("comment with \\\"quote\\\"", spy.getComment(true));

        properties.remove("comment");
        Assertions.assertEquals("", spy.getComment());
    }

    /** Creates a mock Transform with the given canonical toString() value.
     *  Also stubs isIdentity() and isVoid() based on the value. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Transform mockTransform(String toStringValue) {
        Transform t = Mockito.mock(Transform.class);
        Mockito.when(t.toString()).thenReturn(toStringValue);
        Mockito.when(t.isIdentity()).thenReturn("identity".equals(toStringValue));
        Mockito.when(t.isVoid()).thenReturn("void".equals(toStringValue));
        return t;
    }

    @SuppressWarnings("rawtypes")
    private void setupSingleField(Transform transform, String colName) {
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(spec.fields()).thenReturn(Lists.newArrayList(field));
        Mockito.when(field.sourceId()).thenReturn(1);
        Mockito.when(schema.findColumnName(1)).thenReturn(colName);
        Mockito.doReturn(transform).when(field).transform();
    }

    // ── getPartitionSpecSql tests ───────────────────────────────────────────

    @Test
    public void testGetPartitionSpecSqlNullSpec() {
        IcebergExternalTable spy = createSpyTable();
        Mockito.when(icebergTable.spec()).thenReturn(null);
        Assertions.assertEquals("", spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlUnpartitioned() {
        IcebergExternalTable spy = createSpyTable();
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(true);
        Assertions.assertEquals("", spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlIdentity() {
        IcebergExternalTable spy = createSpyTable();
        setupSingleField(mockTransform("identity"), "d_year");
        Assertions.assertEquals("PARTITION BY LIST (`d_year`) ()", spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlBucket() {
        IcebergExternalTable spy = createSpyTable();
        setupSingleField(mockTransform("bucket[2048]"), "ss_item_sk");
        Assertions.assertEquals("PARTITION BY LIST (BUCKET(2048, `ss_item_sk`)) ()",
                spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlTruncate() {
        IcebergExternalTable spy = createSpyTable();
        setupSingleField(mockTransform("truncate[10]"), "category");
        Assertions.assertEquals("PARTITION BY LIST (TRUNCATE(10, `category`)) ()",
                spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlTimeTransforms() {
        IcebergExternalTable spy = createSpyTable();
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(spec.fields()).thenReturn(Lists.newArrayList(field));
        Mockito.when(field.sourceId()).thenReturn(1);
        Mockito.when(schema.findColumnName(ArgumentMatchers.anyInt())).thenReturn("ts");

        Mockito.doReturn(mockTransform("year")).when(field).transform();
        Assertions.assertEquals("PARTITION BY LIST (YEAR(`ts`)) ()", spy.getPartitionSpecSql());

        Mockito.doReturn(mockTransform("month")).when(field).transform();
        Assertions.assertEquals("PARTITION BY LIST (MONTH(`ts`)) ()", spy.getPartitionSpecSql());

        Mockito.doReturn(mockTransform("day")).when(field).transform();
        Assertions.assertEquals("PARTITION BY LIST (DAY(`ts`)) ()", spy.getPartitionSpecSql());

        Mockito.doReturn(mockTransform("hour")).when(field).transform();
        Assertions.assertEquals("PARTITION BY LIST (HOUR(`ts`)) ()", spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlVoidSkipped() {
        IcebergExternalTable spy = createSpyTable();
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(spec.fields()).thenReturn(Lists.newArrayList(field));
        Mockito.when(field.sourceId()).thenReturn(1);
        Mockito.when(schema.findColumnName(1)).thenReturn("ts");
        Mockito.doReturn(mockTransform("void")).when(field).transform();
        Assertions.assertEquals("", spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlMultipleFields() {
        IcebergExternalTable spy = createSpyTable();
        PartitionField field2 = Mockito.mock(PartitionField.class);

        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(spec.fields()).thenReturn(Lists.newArrayList(field, field2));
        Mockito.when(field.sourceId()).thenReturn(1);
        Mockito.when(schema.findColumnName(1)).thenReturn("sold_date_sk");
        Mockito.doReturn(mockTransform("identity")).when(field).transform();
        Mockito.when(field2.sourceId()).thenReturn(2);
        Mockito.when(schema.findColumnName(2)).thenReturn("item_sk");
        Mockito.doReturn(mockTransform("bucket[128]")).when(field2).transform();

        Assertions.assertEquals("PARTITION BY LIST (`sold_date_sk`, BUCKET(128, `item_sk`)) ()",
                spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlReservedWordColumnQuoted() {
        // Reserved SQL keyword as column name must be backtick-quoted for replayable DDL.
        IcebergExternalTable spy = createSpyTable();
        setupSingleField(mockTransform("identity"), "select");
        Assertions.assertEquals("PARTITION BY LIST (`select`) ()", spy.getPartitionSpecSql());
    }

    @Test
    public void testGetPartitionSpecSqlUnresolvableColumnSkipped() {
        IcebergExternalTable spy = createSpyTable();
        int unknownSourceId = 999;
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Mockito.when(spec.fields()).thenReturn(Lists.newArrayList(field));
        Mockito.when(field.sourceId()).thenReturn(unknownSourceId);
        Mockito.when(schema.findColumnName(unknownSourceId)).thenReturn(null);
        Assertions.assertEquals("", spy.getPartitionSpecSql());
    }
}
