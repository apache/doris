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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.paimon.PaimonPartitionInfoLoader;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TSchema;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonUtilTest {
    private static final String TABLE_READ_SEQUENCE_NUMBER_ENABLED = "table-read.sequence-number.enabled";

    private static Table mockPartitionTable(Map<String, String> options, DataField... partitionFields) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.name()).thenReturn("mock_table");
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.stream(partitionFields)
                .map(DataField::name).collect(Collectors.toList()));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(partitionFields));
        Mockito.when(table.options()).thenReturn(options);
        return table;
    }

    private static BinaryRow stringPartitionRow(String... values) {
        BinaryRow row = new BinaryRow(values.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                writer.setNullAt(i);
            } else {
                writer.writeString(i, BinaryString.fromString(values[i]));
            }
        }
        writer.complete();
        return row;
    }

    private static PartitionEntry partitionEntry(BinaryRow partition, long sequence) {
        return new PartitionEntry(partition, sequence, sequence, sequence, sequence, 1);
    }

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
    public void testVariantMapsToComputeV2() {
        Type type = PaimonUtil.paimonTypeToDorisType(
                new org.apache.paimon.types.VariantType(), true, true);

        Assert.assertTrue(type.isVariantType());
        Assert.assertTrue(((VariantType) type).isComputeV2());
    }

    @Test
    public void testTimestampWriteTypeMappingUsesDateTimeV2() {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "ntz", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(1, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                DataTypes.FIELD(2, "nested_ltz",
                        DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))));

        StructType writeType = (StructType) PaimonUtil.paimonTypeToDorisType(rowType, false, false);

        Assert.assertEquals(PrimitiveType.DATETIMEV2,
                writeType.getFields().get(0).getType().getPrimitiveType());
        Assert.assertEquals(PrimitiveType.DATETIMEV2,
                writeType.getFields().get(1).getType().getPrimitiveType());
        ArrayType nestedLtz = (ArrayType) writeType.getFields().get(2).getType();
        Assert.assertEquals(PrimitiveType.DATETIMEV2,
                nestedLtz.getItemType().getPrimitiveType());
    }

    @Test
    public void testVariantTypeMappingIncludesNestedTypes() {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "direct", DataTypes.VARIANT()),
                DataTypes.FIELD(1, "nested", DataTypes.ARRAY(DataTypes.VARIANT())));

        StructType writeType = (StructType) PaimonUtil.paimonTypeToDorisType(rowType, false, false);

        Assert.assertTrue(writeType.getFields().get(0).getType().isVariantType());
        Assert.assertTrue(((ArrayType) writeType.getFields().get(1).getType())
                .getItemType().isVariantType());
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

        Map<String, String> partitionInfoMap = PaimonUtil.getPartitionInfoMap(
                table, partitionValues, "UTC");

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
    public void testSystemTableSchemaPreservesNonLowercaseColumnNames() {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "_ROW_ID", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "_SEQUENCE_NUMBER", DataTypes.BIGINT()));

        List<Column> columns = PaimonSysExternalTable.buildFullSchema(
                rowType.getFields(), false, false);

        Assert.assertEquals("_ROW_ID", columns.get(0).getName());
        Assert.assertEquals("_SEQUENCE_NUMBER", columns.get(1).getName());
    }

    @Test
    public void testParseSchemaPreservesNestedFieldMetadata() {
        DataField eventTime = DataTypes.FIELD(
                17, "event_time", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(5, "payload", DataTypes.ROW(eventTime)));

        List<Column> columns = PaimonUtil.parseSchema(rowType, Collections.emptyList(), false, true);

        Assert.assertEquals(5, columns.get(0).getUniqueId());
        Column nested = columns.get(0).getChildren().get(0);
        Assert.assertEquals(17, nested.getUniqueId());
        Assert.assertEquals("WITH_TIMEZONE", nested.getExtraInfo());
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
        Table table = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "source", DataTypes.STRING()),
                DataTypes.FIELD(1, "part_str", DataTypes.STRING()),
                DataTypes.FIELD(2, "pass", DataTypes.STRING()));
        PartitionEntry partition = partitionEntry(stringPartitionRow(
                "dataset/team-a/segment-01", "/ymd=20260701/hour=[0-9][0-9]/*.jsonl", "s1"), 1L);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table, partitionColumns, Collections.singletonList(partition));

        Assert.assertEquals(PaimonPartitionInfo.PruningStatus.PRUNABLE, partitionInfo.getPruningStatus());
        Assert.assertEquals(1, partitionInfo.getNameToPartition().size());
        Assert.assertEquals(1, partitionInfo.getNameToPartitionItem().size());
        String partitionName = "source=dataset%2Fteam-a%2Fsegment-01"
                + "/part_str=%2Fymd%3D20260701%2Fhour%3D%5B0-9%5D%5B0-9%5D%2F%2A.jsonl/pass=s1";
        Assert.assertTrue(partitionInfo.getNameToPartition().containsKey(partitionName));
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
        Table table = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "source", DataTypes.STRING()),
                DataTypes.FIELD(1, "part_str", DataTypes.STRING()),
                DataTypes.FIELD(2, "pass", DataTypes.STRING()));
        PartitionEntry partition = partitionEntry(stringPartitionRow(
                "dataset/team-a/segment-01", "/ymd=20260721", "s1"), 1L);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table, partitionColumns, Collections.singletonList(partition));

        String partitionName = "source=dataset%2Fteam-a%2Fsegment-01"
                + "/part_str=%2Fymd%3D20260721/pass=s1";
        Assert.assertTrue(partitionInfo.getNameToPartition().containsKey(partitionName));
        PartitionItem partitionItem = partitionInfo.getNameToPartitionItem().get(partitionName);
        List<String> actualValues = ((ListPartitionItem) partitionItem).getItems().get(0)
                .getPartitionValuesAsStringList();
        Assert.assertEquals(Arrays.asList(
                "dataset/team-a/segment-01", "/ymd=20260721", "s1"), actualValues);
    }

    @Test
    public void testGeneratePartitionInfoUsesLegacyDateName() {
        List<Column> partitionColumns = Collections.singletonList(new Column("dt", Type.DATEV2));
        Table table = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "dt", DataTypes.DATE()));
        PartitionEntry partition = partitionEntry(BinaryRow.singleColumn(19737), 1L);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table, partitionColumns, Collections.singletonList(partition));

        String partitionName = "dt=19737";
        Assert.assertTrue(partitionInfo.getNameToPartition().containsKey(partitionName));
        PartitionItem partitionItem = partitionInfo.getNameToPartitionItem().get(partitionName);
        Assert.assertEquals(Collections.singletonList("2024-01-15"),
                ((ListPartitionItem) partitionItem).getItems().get(0).getPartitionValuesAsStringList());
    }

    @Test
    public void testGeneratePartitionInfoUsesCanonicalDateNameWhenLegacyDisabled() {
        List<Column> partitionColumns = Collections.singletonList(new Column("dt", Type.DATEV2));
        Table table = mockPartitionTable(Collections.singletonMap("partition.legacy-name", "false"),
                DataTypes.FIELD(0, "dt", DataTypes.DATE()));
        PartitionEntry partition = partitionEntry(BinaryRow.singleColumn(19737), 1L);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table, partitionColumns, Collections.singletonList(partition));

        String partitionName = "dt=2024-01-15";
        Assert.assertTrue(partitionInfo.getNameToPartition().containsKey(partitionName));
        PartitionItem partitionItem = partitionInfo.getNameToPartitionItem().get(partitionName);
        Assert.assertEquals(Collections.singletonList("2024-01-15"),
                ((ListPartitionItem) partitionItem).getItems().get(0).getPartitionValuesAsStringList());
    }

    @Test
    public void testGeneratePartitionInfoUsesCollisionFreePartitionNames() {
        List<Column> partitionColumns = Arrays.asList(
                new Column("a", Type.STRING),
                new Column("b", Type.STRING));
        Table table = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "a", DataTypes.STRING()),
                DataTypes.FIELD(1, "b", DataTypes.STRING()));
        PartitionEntry firstPartition = partitionEntry(stringPartitionRow("x/b=y", "z"), 1L);
        PartitionEntry secondPartition = partitionEntry(stringPartitionRow("x", "y/b=z"), 2L);

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table, partitionColumns, Arrays.asList(firstPartition, secondPartition));

        String firstPartitionName = "a=x%2Fb%3Dy/b=z";
        String secondPartitionName = "a=x/b=y%2Fb%3Dz";
        Assert.assertEquals(PaimonPartitionInfo.PruningStatus.PRUNABLE, partitionInfo.getPruningStatus());
        Assert.assertEquals(2, partitionInfo.getNameToPartition().size());
        Assert.assertEquals(2, partitionInfo.getNameToPartitionItem().size());
        Assert.assertEquals(1L, partitionInfo.getNameToPartition().get(firstPartitionName).recordCount());
        Assert.assertEquals(2L, partitionInfo.getNameToPartition().get(secondPartitionName).recordCount());
        Assert.assertEquals(Arrays.asList("x/b=y", "z"),
                ((ListPartitionItem) partitionInfo.getNameToPartitionItem().get(firstPartitionName))
                        .getItems().get(0).getPartitionValuesAsStringList());
        Assert.assertEquals(Arrays.asList("x", "y/b=z"),
                ((ListPartitionItem) partitionInfo.getNameToPartitionItem().get(secondPartitionName))
                        .getItems().get(0).getPartitionValuesAsStringList());
    }

    @Test
    public void testGeneratePartitionInfoRejectsDuplicatePartitionNames() {
        List<Column> partitionColumns = Collections.singletonList(new Column("part", Type.STRING));
        Table table = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "part", DataTypes.STRING()));
        PartitionEntry firstPartition = partitionEntry(stringPartitionRow("same"), 1L);
        PartitionEntry secondPartition = partitionEntry(stringPartitionRow("same"), 2L);

        IllegalStateException exception = Assert.assertThrows(IllegalStateException.class,
                () -> PaimonUtil.generatePartitionInfo(
                        table, partitionColumns, Arrays.asList(firstPartition, secondPartition)));

        Assert.assertTrue(exception.getMessage().contains("Duplicate typed Paimon partition"));
    }

    @Test
    public void testGeneratePartitionInfoReturnsUnprunableWithoutPartialMaps() {
        List<Column> partitionColumns = Collections.singletonList(new Column("part", Type.INT));
        Table table = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "part", DataTypes.STRING()));
        List<PartitionEntry> partitions = Arrays.asList(
                partitionEntry(stringPartitionRow("1"), 1L),
                partitionEntry(stringPartitionRow("not-an-int"), 2L));

        PaimonPartitionInfo partitionInfo =
                PaimonUtil.generatePartitionInfo(table, partitionColumns, partitions);

        Assert.assertSame(PaimonPartitionInfo.UNPRUNABLE, partitionInfo);
        Assert.assertEquals(PaimonPartitionInfo.PruningStatus.UNPRUNABLE, partitionInfo.getPruningStatus());
        Assert.assertTrue(partitionInfo.getNameToPartition().isEmpty());
        Assert.assertTrue(partitionInfo.getNameToPartitionItem().isEmpty());
    }

    @Test
    public void testGeneratePartitionInfoDoesNotCacheSessionZonedLtzBounds() {
        List<Column> partitionColumns =
                Collections.singletonList(new Column("part", Type.DATETIMEV2));
        Table table = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "part",
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table,
                partitionColumns,
                Collections.singletonList(Mockito.mock(PartitionEntry.class)));

        Assert.assertSame(PaimonPartitionInfo.UNPRUNABLE, partitionInfo);
    }

    @Test
    public void testGeneratePartitionInfoSupportsTimestampWithoutTimeZone() {
        List<Column> partitionColumns = Collections.singletonList(
                new Column("part", org.apache.doris.catalog.ScalarType.createDatetimeV2Type(6)));
        DataField partitionField = DataTypes.FIELD(0, "part", DataTypes.TIMESTAMP(9));
        Table table = mockPartitionTable(Collections.emptyMap(), partitionField);
        BinaryRow partitionRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partitionRow);
        writer.writeTimestamp(0, Timestamp.fromLocalDateTime(
                LocalDateTime.of(2026, 7, 29, 12, 34, 56, 123456789)), 9);
        writer.complete();

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table, partitionColumns,
                Collections.singletonList(partitionEntry(partitionRow, 1L)));

        Assert.assertEquals(PaimonPartitionInfo.PruningStatus.PRUNABLE,
                partitionInfo.getPruningStatus());
        PartitionItem partitionItem =
                partitionInfo.getNameToPartitionItem().values().iterator().next();
        Assert.assertEquals(Collections.singletonList("2026-07-29 12:34:56.123456"),
                ((ListPartitionItem) partitionItem).getItems().get(0)
                        .getPartitionValuesAsStringList());
    }

    @Test
    public void testGeneratePartitionInfoSupportsTimestampWithoutFraction() {
        List<Column> partitionColumns = Collections.singletonList(
                new Column("part", org.apache.doris.catalog.ScalarType.createDatetimeV2Type(0)));
        DataField partitionField = DataTypes.FIELD(0, "part", DataTypes.TIMESTAMP(0));
        Table table = mockPartitionTable(Collections.emptyMap(), partitionField);
        BinaryRow partitionRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partitionRow);
        writer.writeTimestamp(0, Timestamp.fromLocalDateTime(
                LocalDateTime.of(2026, 7, 29, 12, 34, 56)), 0);
        writer.complete();

        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(
                table, partitionColumns,
                Collections.singletonList(partitionEntry(partitionRow, 1L)));

        Assert.assertEquals(PaimonPartitionInfo.PruningStatus.PRUNABLE,
                partitionInfo.getPruningStatus());
        PartitionItem partitionItem =
                partitionInfo.getNameToPartitionItem().values().iterator().next();
        Assert.assertEquals(Collections.singletonList("2026-07-29 12:34:56"),
                ((ListPartitionItem) partitionItem).getItems().get(0)
                        .getPartitionValuesAsStringList());
    }

    @Test
    public void testGeneratePartitionInfoReturnsUnprunableForAmbiguousDisplayNames() {
        String defaultPartitionName = "__CUSTOM_DEFAULT_PARTITION__";
        List<Column> partitionColumns = Collections.singletonList(new Column("region", Type.STRING));
        Table table = mockPartitionTable(
                Collections.singletonMap("partition.default-name", defaultPartitionName),
                DataTypes.FIELD(0, "region", DataTypes.STRING()));
        List<PartitionEntry> partitions = Arrays.asList(
                partitionEntry(stringPartitionRow((String) null), 1L),
                partitionEntry(stringPartitionRow(""), 2L),
                partitionEntry(stringPartitionRow("null"), 3L),
                partitionEntry(stringPartitionRow(defaultPartitionName), 4L));

        PaimonPartitionInfo partitionInfo =
                PaimonUtil.generatePartitionInfo(table, partitionColumns, partitions);

        Assert.assertSame(PaimonPartitionInfo.UNPRUNABLE, partitionInfo);
        Assert.assertTrue(partitionInfo.getNameToPartition().isEmpty());
        Assert.assertTrue(partitionInfo.getNameToPartitionItem().isEmpty());
    }

    @Test
    public void testPartitionInfoLoaderUsesSnapshotTableEntries() throws Exception {
        List<Column> partitionColumns =
                Collections.singletonList(new Column("region", Type.STRING));
        Table snapshotTable = mockPartitionTable(Collections.emptyMap(),
                DataTypes.FIELD(0, "region", DataTypes.STRING()));
        ReadBuilder readBuilder = Mockito.mock(ReadBuilder.class);
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(snapshotTable.newReadBuilder()).thenReturn(readBuilder);
        Mockito.when(readBuilder.newScan()).thenReturn(tableScan);
        Mockito.when(tableScan.listPartitionEntries()).thenReturn(
                Collections.singletonList(partitionEntry(stringPartitionRow("east"), 1L)));

        NameMapping nameMapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
        PaimonPartitionInfo partitionInfo =
                new PaimonPartitionInfoLoader().load(nameMapping, snapshotTable, partitionColumns);

        Assert.assertEquals(PaimonPartitionInfo.PruningStatus.PRUNABLE, partitionInfo.getPruningStatus());
        Assert.assertTrue(partitionInfo.getNameToPartitionItem().containsKey("region=east"));
        Mockito.verify(tableScan).listPartitionEntries();
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
