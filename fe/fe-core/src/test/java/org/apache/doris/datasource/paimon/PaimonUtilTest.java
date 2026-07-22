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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TSchema;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PaimonUtilTest {
    private static final String TABLE_READ_SEQUENCE_NUMBER_ENABLED = "table-read.sequence-number.enabled";

    @Test
    public void testSchemaForVarcharAndChar() {
        DataField c1 = new DataField(1, "c1", new VarCharType(32));
        DataField c2 = new DataField(2, "c2", new CharType(14));
        Type type1 = PaimonUtil.paimonTypeToDorisType(c1.type(), true, true);
        Type type2 = PaimonUtil.paimonTypeToDorisType(c2.type(), true, true);
        Assert.assertTrue(type1.isVarchar());
        Assert.assertEquals(32, type1.getLength());
        Assert.assertEquals(14, type2.getLength());
    }

    @Test
    public void testGetPartitionInfoMapSupportsFloatingPointPartitions() {
        DataField floatPartition = DataTypes.FIELD(0, "float_partition", DataTypes.FLOAT());
        DataField doublePartition = DataTypes.FIELD(1, "double_partition", DataTypes.DOUBLE());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.name()).thenReturn("mock_table");
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.asList("float_partition", "double_partition"));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(floatPartition, doublePartition));

        float floatValue = Math.nextUp(0.1F);
        double doubleValue = Math.nextUp(0.1D);
        BinaryRow partitionValues = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partitionValues);
        writer.writeFloat(0, floatValue);
        writer.writeDouble(1, doubleValue);
        writer.complete();

        Map<String, String> partitionInfoMap = PaimonUtil.getPartitionInfoMap(table, partitionValues, "UTC");

        String serializedFloat = partitionInfoMap.get("float_partition");
        String serializedDouble = partitionInfoMap.get("double_partition");
        Assert.assertEquals(Float.toString(floatValue), serializedFloat);
        Assert.assertEquals(Double.toString(doubleValue), serializedDouble);
        Assert.assertEquals(Float.floatToIntBits(floatValue),
                Float.floatToIntBits(Float.parseFloat(serializedFloat)));
        Assert.assertEquals(Double.doubleToLongBits(doubleValue),
                Double.doubleToLongBits(Double.parseDouble(serializedDouble)));
    }

    @Test
    public void testParseSchemaPreservesNonLowercaseColumnNames() {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "mIxEd_COL", DataTypes.INT()),
                DataTypes.FIELD(1, "PART", DataTypes.STRING()));

        List<Column> columns = PaimonUtil.parseSchema(rowType, Collections.singletonList("PART"), false, false);

        Assert.assertEquals("mIxEd_COL", columns.get(0).getName());
        Assert.assertEquals("PART", columns.get(1).getName());
        Assert.assertTrue(columns.get(1).isKey());
    }

    @Test
    public void testGetPartitionInfoMapPreservesNonLowercaseKeys() {
        DataField mixedCasePartition = DataTypes.FIELD(0, "Dt", DataTypes.STRING());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.name()).thenReturn("mock_table");
        Mockito.when(table.partitionKeys()).thenReturn(Collections.singletonList("Dt"));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(mixedCasePartition));

        BinaryRow partitionValues = BinaryRow.singleColumn(BinaryString.fromString("2026-05-26"));

        Map<String, String> partitionInfoMap = PaimonUtil.getPartitionInfoMap(table, partitionValues, "UTC");

        Assert.assertFalse(partitionInfoMap.containsKey("dt"));
        Assert.assertEquals("2026-05-26", partitionInfoMap.get("Dt"));
    }

    @Test
    public void testGeneratePartitionInfoWithSpecialCharacters() {
        List<Column> partitionColumns = Arrays.asList(
                new Column("source", Type.STRING),
                new Column("part_str", Type.STRING),
                new Column("pass", Type.STRING));
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("source", "dataset/team-a/segment-01");
        spec.put("part_str", "/ymd=20260701/hour=[0-9][0-9]/*.jsonl");
        spec.put("pass", "s1");
        Partition partition = new Partition(spec, 1L, 1L, 1L, 1L, false);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                partitionColumns, Collections.singletonList(partition), false);

        Assert.assertFalse(partitionInfo.isPartitionInvalid());
        Assert.assertEquals(1, partitionInfo.getNameToPartition().size());
        Assert.assertEquals(1, partitionInfo.getNameToPartitionItem().size());
        PartitionItem partitionItem = partitionInfo.getNameToPartitionItem().values().iterator().next();
        List<String> actualValues = ((ListPartitionItem) partitionItem).getItems().get(0)
                .getPartitionValuesAsStringList();
        Assert.assertEquals(Arrays.asList(
                "dataset/team-a/segment-01",
                "/ymd=20260701/hour=[0-9][0-9]/*.jsonl",
                "s1"), actualValues);
    }

    @Test
    public void testGeneratePartitionInfoUsesPartitionColumnOrder() {
        List<Column> partitionColumns = Arrays.asList(
                new Column("source", Type.STRING),
                new Column("part_str", Type.STRING),
                new Column("pass", Type.STRING));
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("pass", "s1");
        spec.put("part_str", "/ymd=20260721");
        spec.put("source", "dataset/team-a/segment-01");
        Partition partition = new Partition(spec, 1L, 1L, 1L, 1L, false);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                partitionColumns, Collections.singletonList(partition), false);

        String partitionName = "source=dataset/team-a/segment-01"
                + "/part_str=/ymd=20260721/pass=s1";
        Assert.assertTrue(partitionInfo.getNameToPartition().containsKey(partitionName));
        PartitionItem partitionItem = partitionInfo.getNameToPartitionItem().get(partitionName);
        List<String> actualValues = ((ListPartitionItem) partitionItem).getItems().get(0)
                .getPartitionValuesAsStringList();
        Assert.assertEquals(Arrays.asList(
                "dataset/team-a/segment-01", "/ymd=20260721", "s1"), actualValues);
    }

    @Test
    public void testGeneratePartitionInfoPreservesLegacyDateConversion() {
        List<Column> partitionColumns = Collections.singletonList(new Column("dt", Type.DATEV2));
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("dt", "19737");
        Partition partition = new Partition(spec, 1L, 1L, 1L, 1L, false);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                partitionColumns, Collections.singletonList(partition), true);

        String partitionName = "dt=2024-01-15";
        Assert.assertTrue(partitionInfo.getNameToPartition().containsKey(partitionName));
        PartitionItem partitionItem = partitionInfo.getNameToPartitionItem().get(partitionName);
        Assert.assertEquals(Collections.singletonList("2024-01-15"),
                ((ListPartitionItem) partitionItem).getItems().get(0).getPartitionValuesAsStringList());
    }

    @Test
    public void testBinlogHistorySchemaWithSequenceNumber() {
        PaimonSysExternalTable binlogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(binlogTable.getSysTableType()).thenReturn("binlog");
        Mockito.when(binlogTable.getTableProperties()).thenReturn(
                Collections.singletonMap(TABLE_READ_SEQUENCE_NUMBER_ENABLED, "true"));
        Mockito.when(binlogTable.getName()).thenReturn("mock_binlog");

        List<DataField> sourceFields = Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "name", DataTypes.STRING()));
        TableSchema sourceSchema = new TableSchema(1L, sourceFields, 1, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), "");
        TSchema historySchema = PaimonUtil.getHistorySchemaInfo(binlogTable, sourceSchema, true, true);
        List<TFieldPtr> fields = historySchema.getRootField().getFields();

        Assert.assertEquals("rowkind", fields.get(0).getFieldPtr().getName());
        Assert.assertEquals("_SEQUENCE_NUMBER", fields.get(1).getFieldPtr().getName());
        Assert.assertEquals("id", fields.get(2).getFieldPtr().getName());
        Assert.assertEquals(TPrimitiveType.ARRAY, fields.get(2).getFieldPtr().getType().getType());
        Assert.assertEquals("name", fields.get(3).getFieldPtr().getName());
        Assert.assertEquals(TPrimitiveType.ARRAY, fields.get(3).getFieldPtr().getType().getType());
    }

    @Test
    public void testAuditLogHistorySchemaWithoutSequenceNumber() {
        PaimonSysExternalTable auditLogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(auditLogTable.getSysTableType()).thenReturn("audit_log");
        Mockito.when(auditLogTable.getTableProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(auditLogTable.getName()).thenReturn("mock_audit_log");

        List<DataField> sourceFields = Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "name", DataTypes.STRING()));
        TableSchema sourceSchema = new TableSchema(1L, sourceFields, 1, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), "");
        TSchema historySchema = PaimonUtil.getHistorySchemaInfo(auditLogTable, sourceSchema, true, true);
        List<TFieldPtr> fields = historySchema.getRootField().getFields();

        Assert.assertEquals(3, fields.size());
        Assert.assertEquals("rowkind", fields.get(0).getFieldPtr().getName());
        Assert.assertEquals("id", fields.get(1).getFieldPtr().getName());
        Assert.assertEquals("name", fields.get(2).getFieldPtr().getName());
    }
}
