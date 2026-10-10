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
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TypeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * How a static partition reaches Paimon's static overwrite. The written rows carry each PARTITION literal cast
 * to its column type, so the overwrite must name the partition by that cast value, in the form Paimon parses:
 * a value spelled differently selects another partition, or none, and the commit then fails or replaces the
 * wrong rows.
 */
public class PaimonWriteBindingTest {

    private static final String DEFAULT_NAME = "__CUSTOM_DEFAULT__";

    private TimeZone jvmZone;

    @BeforeEach
    public void pinJvmZone() {
        // Paimon parses a static LTZ value in the JVM zone. Keep it away from every session zone used below, so
        // a value left in the session zone cannot pass by accident.
        jvmZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    }

    @AfterEach
    public void restoreJvmZone() {
        TimeZone.setDefault(jvmZone);
    }

    @Test
    public void staticPartitionUsesTheValueCastToTheColumnType(@TempDir Path warehouse) {
        // PARTITION (part = true) on an INT column and PARTITION (dt = 20240101) on a DATE column: Paimon reads
        // "true" as no INT and "20240101" as the 20240101st day after the epoch. MUTATION: resolving from the spec
        // as written -> both red.
        FileStoreTable intTable = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.INT()));
        Assertions.assertEquals("1", PaimonWriteBinding.resolveStaticPartition(intTable,
                write(true, "part", "true", "1", ConnectorType.of("INT")), "UTC").get("part"));

        FileStoreTable dateTable = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "dt", DataTypes.DATE()));
        Assertions.assertEquals("2024-01-01", PaimonWriteBinding.resolveStaticPartition(dateTable,
                write(true, "dt", "20240101", "2024-01-01", ConnectorType.of("DATEV2")), "UTC").get("dt"));
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
                        write(true, "part", DEFAULT_NAME, DEFAULT_NAME, ConnectorType.of("STRING")), "UTC"));
        Assertions.assertTrue(error.getMessage().contains("cannot be represented"), error.getMessage());

        // An INSERT does not overwrite anything, and its rows keep the value as written.
        Assertions.assertEquals(DEFAULT_NAME, PaimonWriteBinding.resolveStaticPartition(table,
                write(false, "part", DEFAULT_NAME, DEFAULT_NAME, ConnectorType.of("STRING")), "UTC").get("part"));
    }

    @Test
    public void staticLtzPartitionMovesFromTheSessionZoneToTheSdkZone(@TempDir Path warehouse) {
        // Mapped to DATETIMEV2, an LTZ literal is local time in the session zone; Paimon parses the overwrite
        // value in the JVM zone. MUTATION: passing the value through -> red (Los Angeles is not Shanghai).
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        Instant written = Instant.parse("2024-01-15T00:30:45.123456Z");

        for (String sessionZone : Arrays.asList("Asia/Shanghai", "CST", "+08:00")) {
            String value = PaimonWriteBinding.resolveStaticPartition(table,
                    write(true, "part", "2024-01-15 08:30:45.123456", "2024-01-15 08:30:45.123456",
                            ConnectorType.of("DATETIMEV2", 6, 0)), sessionZone).get("part");

            Assertions.assertEquals("2024-01-14 16:30:45.123456", value, sessionZone);
            Assertions.assertEquals(written, parsedByPaimon(table, value), sessionZone);
        }
    }

    @Test
    public void staticTimestampTzPartitionMovesTheInstantToTheSdkZone(@TempDir Path warehouse) {
        // Mapped to TIMESTAMPTZ, the cast value is the instant in UTC with its offset, which Paimon's parser does
        // not take. MUTATION: reading it as session-local time -> red.
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));

        String value = PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", "2024-01-15 08:30:45.123456", "2024-01-15 00:30:45.123456+00:00",
                        ConnectorType.of("TIMESTAMPTZ", 6, 0)), "Asia/Shanghai").get("part");

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
                    () -> PaimonWriteBinding.resolveStaticPartition(table,
                            write(true, "part", instant, instant, ConnectorType.of("TIMESTAMPTZ", 6, 0)), "UTC"));
            Assertions.assertTrue(error.getMessage().contains("ambiguous"), error.getMessage());
            Assertions.assertTrue(error.getMessage().contains("America/Los_Angeles"), error.getMessage());

            // An INSERT does not overwrite anything, so it keeps the value.
            Assertions.assertEquals("2023-11-05 01:30:00.123456", PaimonWriteBinding.resolveStaticPartition(table,
                    write(false, "part", instant, instant, ConnectorType.of("TIMESTAMPTZ", 6, 0)), "UTC")
                    .get("part"));
        }

        // A session-local DATETIMEV2 value lands in the same overlap: 09:30:45 UTC is the second 01:30:45.
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonWriteBinding.resolveStaticPartition(table,
                        write(true, "part", "2024-11-03 09:30:45", "2024-11-03 09:30:45",
                                ConnectorType.of("DATETIMEV2", 0, 0)), "UTC"));
        Assertions.assertTrue(error.getMessage().contains("ambiguous"), error.getMessage());
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
            assertStaticTimestampTzRoundTrip(table, instant);
        }
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        assertStaticTimestampTzRoundTrip(table, "2023-11-05 10:30:00.123456+00:00");
    }

    @Test
    public void staticLtzOverwriteRejectsASessionTimeADstChangeSkips(@TempDir Path warehouse) {
        // 02:30:45 on 2024-03-10 does not exist in Los Angeles. Java moves it to 03:30:45, while the BE writes the
        // rows at the instant of the change (cctz), so no overwrite value names the rows' partition.
        // MUTATION: dropping the check -> red.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        FileStoreTable table = partitionedTable(warehouse, Collections.emptyMap(),
                new DataField(1, "part", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonWriteBinding.resolveStaticPartition(table,
                        write(true, "part", "2024-03-10 02:30:45", "2024-03-10 02:30:45",
                                ConnectorType.of("DATETIMEV2", 0, 0)), "America/Los_Angeles"));
        Assertions.assertTrue(error.getMessage().contains("does not exist"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("America/Los_Angeles"), error.getMessage());

        // An INSERT does not overwrite anything, so it keeps the value.
        Assertions.assertNotNull(PaimonWriteBinding.resolveStaticPartition(table,
                write(false, "part", "2024-03-10 02:30:45", "2024-03-10 02:30:45",
                        ConnectorType.of("DATETIMEV2", 0, 0)), "America/Los_Angeles").get("part"));
    }

    @Test
    public void staticPartitionNullUsesTheDefaultPartitionName(@TempDir Path warehouse) {
        FileStoreTable table = partitionedTable(warehouse,
                Collections.singletonMap("partition.default-name", DEFAULT_NAME),
                new DataField(1, "part", DataTypes.INT()));
        ConnectorWriteHandle write = handle(true, Collections.singletonMap("part", "NULL"),
                Collections.emptyMap(), Collections.singleton("part"),
                Collections.singletonList(column("part", ConnectorType.of("INT"))));

        Assertions.assertEquals(DEFAULT_NAME,
                PaimonWriteBinding.resolveStaticPartition(table, write, "UTC").get("part"));
    }

    private static void assertStaticTimestampTzRoundTrip(FileStoreTable table, String instant) {
        String value = PaimonWriteBinding.resolveStaticPartition(table,
                write(true, "part", instant, instant, ConnectorType.of("TIMESTAMPTZ", 6, 0)), "UTC").get("part");
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

    private static ConnectorColumn column(String name, ConnectorType type) {
        return new ConnectorColumn(name, type, null, true, null);
    }

    private static ConnectorWriteHandle write(boolean overwrite, String name, String writtenValue,
            String castValue, ConnectorType type) {
        return handle(overwrite, Collections.singletonMap(name, writtenValue),
                Collections.singletonMap(name, castValue), Collections.emptySet(),
                Arrays.asList(column("id", ConnectorType.of("INT")), column(name, type)));
    }

    private static ConnectorWriteHandle handle(boolean overwrite, Map<String, String> spec,
            Map<String, String> castSpec, Set<String> nullKeys, List<ConnectorColumn> boundTargetColumns) {
        return new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return null;
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return boundTargetColumns;
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
                return castSpec;
            }
        };
    }
}
