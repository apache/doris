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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TypeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * How a static partition reaches Paimon's static overwrite. The written rows carry each PARTITION literal cast
 * to its column type, so the overwrite must name the partition by that cast value, in the form Paimon parses:
 * a value spelled differently selects another partition, or none, and the commit then fails or replaces the
 * wrong rows.
 */
public class PaimonWriteBindingTest {

    private static final String DEFAULT_NAME = "__CUSTOM_DEFAULT__";
    private static final String DYNAMIC_OVERWRITE = CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key();
    private static final Map<String, String> NEW_PARTITION_NAMES =
            Collections.singletonMap("partition.legacy-name", "false");

    private TimeZone jvmZone;

    @BeforeEach
    public void pinJvmZone() {
        // Paimon parses a static LTZ value in the JVM zone. Keep it away from UTC, the zone of the cast values,
        // so a value passed through unchanged cannot pass by accident.
        jvmZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    }

    @AfterEach
    public void restoreJvmZone() {
        TimeZone.setDefault(jvmZone);
    }

    // configureTableForWrite maps a Doris INSERT OVERWRITE onto Paimon's dynamic-partition-overwrite option. Paimon
    // defaults that option to true, which replaces only the partitions the new rows reach; a Doris full-table
    // overwrite replaces the whole table and a static-partition overwrite replaces exactly the named partition.

    @Test
    public void fullOverwriteUsesStaticSemanticsByDefault(@TempDir Path warehouse) {
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "pt", DataTypes.STRING()));

        FileStoreTable configured = PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.emptyMap());

        // Left at Paimon's default, the overwrite would keep every partition the new rows do not reach.
        Assertions.assertEquals("false", configured.options().get(DYNAMIC_OVERWRITE));
        Assertions.assertFalse(configured.coreOptions().dynamicPartitionOverwrite());
    }

    @Test
    public void fullOverwriteHonorsExplicitDynamicSemantics(@TempDir Path warehouse) {
        // A table that sets the option itself asked for dynamic overwrite, so the binding keeps the
        // loaded table instead of copying it with the static override.
        FileStoreTable table = partitionedTable(warehouse, Collections.singletonMap(DYNAMIC_OVERWRITE, "true"),
                new DataField(1, "pt", DataTypes.STRING()));

        Assertions.assertSame(table, PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.emptyMap()));
    }

    @Test
    public void staticPartitionOverwriteAlwaysUsesStaticSemantics(@TempDir Path warehouse) {
        // A static partition names the partition to replace. Dynamic semantics would leave it intact
        // when the query produces no row for it, so the table option does not apply here.
        FileStoreTable table = partitionedTable(warehouse, Collections.singletonMap(DYNAMIC_OVERWRITE, "true"),
                new DataField(1, "pt", DataTypes.STRING()));

        FileStoreTable configured = PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.singletonMap("pt", "1"));

        Assertions.assertEquals("false", configured.options().get(DYNAMIC_OVERWRITE));
        Assertions.assertFalse(configured.coreOptions().dynamicPartitionOverwrite());
    }

    @Test
    public void staticPartitionUsesTheValueCastToTheColumnType(@TempDir Path warehouse) {
        // PARTITION (part = true) on an INT column and PARTITION (dt = 20240101) on a DATE column: Paimon reads
        // "true" as no INT and "20240101" as the 20240101st day after the epoch. MUTATION: resolving from the spec
        // as written -> both red.
        FileStoreTable intTable = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.INT()));
        Assertions.assertEquals("1", PaimonWriteBinding.resolveStaticPartition(intTable,
                write(true, "part", "true", "1"), true).get("part"));

        FileStoreTable dateTable = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "dt", DataTypes.DATE()));
        Assertions.assertEquals("2024-01-01", PaimonWriteBinding.resolveStaticPartition(dateTable,
                write(true, "dt", "20240101", "2024-01-01"), true).get("dt"));
    }

    @Test
    public void staticOverwriteRejectsTheDefaultPartitionName(@TempDir Path warehouse) {
        // Paimon's static overwrite reads partition.default-name as the NULL partition of every type, so this
        // value would replace the NULL partition. MUTATION: dropping the check -> red.
        FileStoreTable table = partitionedTable(warehouse,
                Collections.singletonMap("partition.default-name", DEFAULT_NAME),
                new DataField(1, "part", DataTypes.STRING()));

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonWriteBinding.resolveStaticPartition(table,
                        write(true, "part", DEFAULT_NAME, DEFAULT_NAME), true));
        Assertions.assertTrue(error.getMessage().contains("cannot be represented"), error.getMessage());
    }

    @Test
    public void staticLtzPartitionMovesTheInstantToTheSdkZone(@TempDir Path warehouse) {
        // Doris binds an LTZ column as TIMESTAMPTZ, so the cast value is the instant in UTC with its offset,
        // which Paimon's parser does not take: it reads an offset-free value as local time in the JVM zone.
        // MUTATION: passing the value through, or dropping its offset without moving it -> red.
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));

        String value = PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", "2024-01-15 08:30:45.123456", "2024-01-15 00:30:45.123456+00:00"), true)
                .get("part");

        Assertions.assertEquals("2024-01-14 16:30:45.123456", value);
        Assertions.assertEquals(Instant.parse("2024-01-15T00:30:45.123456Z"), parsedByPaimon(table, value));
    }

    @Test
    public void staticLtzOverwriteRejectsAnInstantTheSdkZoneRepeats(@TempDir Path warehouse) {
        // 2023-11-05 08:30Z and 09:30Z are both 01:30 in Los Angeles, the JVM zone here. Either value would name
        // the same partition, which Paimon reads as the earlier instant. MUTATION: dropping the check -> red.
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        for (String instant : Arrays.asList("2023-11-05 08:30:00.123456+00:00",
                "2023-11-05 09:30:00.123456+00:00")) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> PaimonWriteBinding.resolveStaticPartition(table, write(true, "part", instant, instant),
                            true));
            Assertions.assertTrue(error.getMessage().contains("ambiguous"), error.getMessage());
            Assertions.assertTrue(error.getMessage().contains("America/Los_Angeles"), error.getMessage());
        }
    }

    @Test
    public void staticLtzPartitionOutsideAnOverlapRoundTripsThroughPaimon(@TempDir Path warehouse) {
        // Both instants that overlap in Los Angeles are distinct in UTC, and an instant after the overlap is
        // distinct in Los Angeles. MUTATION: rejecting every instant near a DST change -> red.
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        for (String instant : Arrays.asList("2023-11-05 08:30:00.123456+00:00",
                "2023-11-05 09:30:00.123456+00:00")) {
            assertStaticLtzRoundTrip(table, instant);
        }
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        assertStaticLtzRoundTrip(table, "2023-11-05 10:30:00.123456+00:00");
    }

    @Test
    public void staticBinaryPartitionIsTheTextOfTheBytesTheRowsCarry(@TempDir Path warehouse) {
        // Paimon's static overwrite takes the UTF-8 bytes of a value's string: "0x4142", as the cast value of a
        // VARBINARY column reaches the connector, names the partition of those six bytes, not of the two bytes
        // 41 42 the rows carry, so an overwrite without rows would report success and leave the partition it was
        // given in place. MUTATION: passing the cast value through -> red.
        Assertions.assertArrayEquals("0x4142".getBytes(StandardCharsets.UTF_8),
                (byte[]) TypeUtils.castFromString("0x4142", DataTypes.BYTES()));
        for (DataType type : Arrays.asList(DataTypes.BYTES(), DataTypes.VARBINARY(2), DataTypes.BINARY(2))) {
            FileStoreTable table = partitionedTable(warehouse, NEW_PARTITION_NAMES, new DataField(1, "part", type));

            String value = PaimonWriteBinding.resolveStaticPartition(table, write(true, "part", "AB", "0x4142"),
                    true).get("part");

            Assertions.assertArrayEquals(new byte[] {0x41, 0x42}, (byte[]) TypeUtils.castFromString(value, type),
                    type.toString());
        }
    }

    @Test
    public void staticBinaryPartitionRejectsBytesThatAreNotUtf8(@TempDir Path warehouse) {
        // No string's UTF-8 bytes are a lone 0xEF, an overlong '/' or an encoded surrogate, so Paimon's static
        // overwrite cannot name such a partition; a lenient decoder would name another one through U+FFFD.
        // MUTATION: decoding with replacement -> red.
        FileStoreTable table = partitionedTable(warehouse, NEW_PARTITION_NAMES,
                new DataField(1, "part", DataTypes.BYTES()));
        for (String castValue : Arrays.asList("0xEF", "0xC0AF", "0xEDA080")) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> PaimonWriteBinding.resolveStaticPartition(table, write(true, "part", castValue, castValue),
                            true));
            Assertions.assertTrue(error.getMessage().contains("is not UTF-8 text"), error.getMessage());
        }
    }

    @Test
    public void staticBinaryPartitionKeepsTheLengthOfTheBytes(@TempDir Path warehouse) {
        // The rows keep their bytes whatever the column length, and Paimon's static overwrite keeps them for a
        // BINARY(n) column too. For a VARBINARY(n) column it fails the commit, after the files are written, when the
        // bytes are longer, so such a value fails while the write is planned. MUTATION: no length check -> red.
        FileStoreTable binary = partitionedTable(warehouse, NEW_PARTITION_NAMES,
                new DataField(1, "part", DataTypes.BINARY(2)));
        for (String castValue : Arrays.asList("0x41", "0x414243")) {
            String value = PaimonWriteBinding.resolveStaticPartition(binary,
                    write(true, "part", castValue, castValue), true).get("part");
            Assertions.assertArrayEquals(HexFormat.of().parseHex(castValue.substring(2)),
                    (byte[]) TypeUtils.castFromString(value, DataTypes.BINARY(2)), castValue);
        }

        FileStoreTable varbinary = partitionedTable(warehouse, NEW_PARTITION_NAMES,
                new DataField(1, "part", DataTypes.VARBINARY(2)));
        Assertions.assertEquals("A", PaimonWriteBinding.resolveStaticPartition(varbinary,
                write(true, "part", "0x41", "0x41"), true).get("part"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TypeUtils.castFromString("ABC", DataTypes.VARBINARY(2)));
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonWriteBinding.resolveStaticPartition(varbinary,
                        write(true, "part", "0x414243", "0x414243"), true));
        Assertions.assertTrue(error.getMessage().contains("has 3 bytes, more than its type VARBINARY(2) holds"),
                error.getMessage());
    }

    @Test
    public void writeOfABinaryPartitionNeedsNewPartitionNames(@TempDir Path warehouse) {
        // With partition.legacy-name = true, the default, Paimon names a binary partition after the Java array
        // object, so the commit of the written files fails and Paimon cannot read the partition back. The write
        // fails while it is planned; a table with new partition names, or without a binary partition, is written.
        // MUTATION: dropping the check -> red.
        PaimonTableHandle handle = new PaimonTableHandle("db", "tbl", Collections.emptyList(),
                Collections.emptyList());
        for (DataType type : Arrays.asList(DataTypes.BYTES(), DataTypes.VARBINARY(2), DataTypes.BINARY(2))) {
            FileStoreTable legacy = partitionedTable(warehouse, Collections.emptyMap(), new DataField(1, "part", type));
            for (boolean overwrite : new boolean[] {false, true}) {
                DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                        () -> PaimonWriteBinding.create(handle, legacy, Collections.emptyMap(),
                                write(overwrite, "part", "AB", "0x4142"), true));
                Assertions.assertTrue(error.getMessage().contains("'partition.legacy-name' = 'false'"),
                        error.getMessage());
            }
            FileStoreTable renamed = partitionedTable(warehouse, NEW_PARTITION_NAMES, new DataField(1, "part", type));
            Assertions.assertEquals("AB", PaimonWriteBinding.create(handle, renamed, Collections.emptyMap(),
                    write(true, "part", "AB", "0x4142"), true).getStaticPartition().get("part"));
        }
        FileStoreTable legacyString = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.STRING()));
        Assertions.assertFalse(PaimonWriteBinding.create(handle, legacyString, Collections.emptyMap(),
                write(false, "part", "AB", "AB"), true).isOverwrite());
    }

    @Test
    public void staticPartitionNullUsesTheDefaultPartitionName(@TempDir Path warehouse) {
        // A binary column too: SQL NULL names its partition by partition.default-name, not by bytes.
        for (DataType type : Arrays.asList(DataTypes.INT(), DataTypes.BYTES())) {
            FileStoreTable table = partitionedTable(warehouse,
                    Collections.singletonMap("partition.default-name", DEFAULT_NAME),
                    new DataField(1, "part", type));
            ConnectorWriteHandle write = handle(true, Collections.singletonMap("part", "NULL"),
                    Collections::emptyMap, Collections.singleton("part"));

            Assertions.assertEquals(DEFAULT_NAME,
                    PaimonWriteBinding.resolveStaticPartition(table, write, true).get("part"), type.toString());
        }
    }

    @Test
    public void staticCharPartitionIsPaddedAsTheRowsAre(@TempDir Path warehouse) {
        // With padding on, the BE writer pads the rows' CHAR values to the column length, while Paimon parses a
        // static partition value as it is: an unpadded 'a' would name another partition than the rows reach, and
        // the commit would fail. Padding counts code points, and a longer value is kept, as it is in the rows.
        // MUTATION: not padding, or counting UTF-16 units -> red.
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.CHAR(3)));
        Assertions.assertEquals("a", TypeUtils.castFromString("a", DataTypes.CHAR(3)).toString());

        Assertions.assertEquals("a  ", PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", "a", "a"), true).get("part"));
        String supplementary = new String(Character.toChars(0x20000));
        Assertions.assertEquals(supplementary + "  ", PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", supplementary, supplementary), true).get("part"));
        Assertions.assertEquals("abcd", PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", "abcd", "abcd"), true).get("part"));
        Assertions.assertEquals("a", PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", "a", "a"), false).get("part"));
    }

    @Test
    public void staticCharPartitionOfAnOrcTableIsPaddedAsTheRowsAre(@TempDir Path warehouse) {
        // The BE writer pads the CHAR values of a table that writes ORC files as it pads those of any table, so the
        // static partition value of such a table is padded as its rows are. MUTATION: leaving the fields of an ORC
        // table unpadded -> red.
        FileStoreTable table = partitionedTable(warehouse,
                Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "orc"),
                new DataField(1, "part", DataTypes.CHAR(3)));

        Assertions.assertEquals("a  ", PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", "a", "a"), true).get("part"));
    }

    @Test
    public void staticRowKindFieldPartitionIsLeftUnpaddedAsTheRowsAre(@TempDir Path warehouse) {
        // The BE writer does not pad the value of the table's rowkind.field, which Paimon reads as a row kind as it
        // is, so the rows of a CHAR(3) rowkind.field partition column carry '+I'. A padded '+I ' would name
        // another partition: a nonempty overwrite would fail and an empty one would clear nothing. The other CHAR
        // partition column is padded as before. MUTATION: padding the rowkind.field value -> red.
        List<DataField> fields = Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "kind", DataTypes.CHAR(3)), new DataField(2, "code", DataTypes.CHAR(3)));
        TableSchema schema = new TableSchema(0L, fields, 2, Arrays.asList("kind", "code"),
                Collections.emptyList(), Collections.singletonMap(CoreOptions.ROWKIND_FIELD.key(), "kind"), "");
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(),
                new org.apache.paimon.fs.Path("file://" + warehouse + "/db.db/tbl"), schema);
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("kind", "+I");
        spec.put("code", "a");

        Map<String, String> partition = PaimonWriteBinding.resolveStaticPartition(table,
                handle(true, spec, () -> spec, Collections.emptySet()), true);

        Assertions.assertEquals("+I", partition.get("kind"));
        Assertions.assertEquals("a  ", partition.get("code"));
    }

    @Test
    public void insertLeavesTheStaticPartitionValuesAlone(@TempDir Path warehouse) {
        // Only a static overwrite reads the static partition. An INSERT writes each row to the partition its own
        // values name, so a value the cast rejects (a row keeps it as NULL, or as a fitting binary) or an
        // overwrite cannot represent must not fail it. MUTATION: resolving the static partition of an INSERT
        // -> red.
        FileStoreTable table = partitionedTable(warehouse,
                Collections.singletonMap("partition.default-name", DEFAULT_NAME),
                new DataField(1, "part", DataTypes.STRING()));
        ConnectorWriteHandle insert = handle(false, Collections.singletonMap("part", DEFAULT_NAME), () -> {
            throw new AssertionError("an INSERT must not cast its static partition values");
        }, Collections.emptySet());

        PaimonWriteBinding binding = PaimonWriteBinding.create(
                new PaimonTableHandle("db", "tbl", Collections.emptyList(), Collections.emptyList()),
                table, Collections.emptyMap(), insert, true);

        Assertions.assertFalse(binding.isOverwrite());
        Assertions.assertTrue(binding.getStaticPartition().isEmpty(), binding.getStaticPartition().toString());
    }

    private static void assertStaticLtzRoundTrip(FileStoreTable table, String instant) {
        String value = PaimonWriteBinding.resolveStaticPartition(table, write(true, "part", instant, instant), true)
                .get("part");
        Assertions.assertEquals(OffsetDateTime.parse(instant.replace(' ', 'T')).toInstant(),
                parsedByPaimon(table, value), instant);
    }

    private static Instant parsedByPaimon(FileStoreTable table, String value) {
        return ((Timestamp) TypeUtils.castFromString(value, table.rowType().getTypeAt(1))).toInstant();
    }

    private static FileStoreTable partitionedTable(Path warehouse, Map<String, String> options,
            DataField partition) {
        List<DataField> fields = Arrays.asList(new DataField(0, "id", DataTypes.INT()), partition);
        TableSchema schema = new TableSchema(0L, fields, 1, Collections.singletonList(partition.name()),
                Collections.emptyList(), options, "");
        return FileStoreTableFactory.create(LocalFileIO.create(),
                new org.apache.paimon.fs.Path("file://" + warehouse + "/db.db/tbl"), schema);
    }

    private static ConnectorWriteHandle write(boolean overwrite, String name, String writtenValue,
            String castValue) {
        return handle(overwrite, Collections.singletonMap(name, writtenValue),
                () -> Collections.singletonMap(name, castValue), Collections.emptySet());
    }

    private static ConnectorWriteHandle handle(boolean overwrite, Map<String, String> spec,
            Supplier<Map<String, String>> castSpec, Set<String> nullKeys) {
        return new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return null;
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return Collections.emptyList();
            }

            @Override
            public boolean isOverwrite() {
                return overwrite;
            }

            @Override
            public Map<String, String> getStaticPartitionSpec() {
                return spec;
            }

            @Override
            public Set<String> getStaticPartitionNullKeys() {
                return nullKeys;
            }

            @Override
            public Map<String, String> getCastStaticPartitionSpec() {
                return castSpec.get();
            }
        };
    }
}
