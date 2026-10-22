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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

public class PaimonRustReaderEvolutionTest {
    @TempDir
    java.nio.file.Path temporaryFolder;

    @Test
    public void testTimestampEvolutionIntoNanosecondRangeFallback() throws Exception {
        for (int oldPrecision : new int[] {0, 3, 6}) {
            for (int newPrecision : new int[] {7, 8, 9}) {
                assertNestedEvolution(DataTypes.TIMESTAMP(oldPrecision), DataTypes.TIMESTAMP(newPrecision), false);
                assertNestedEvolution(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(oldPrecision),
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(newPrecision), false);
            }
        }
        assertNestedEvolution(DataTypes.TIMESTAMP(7), DataTypes.TIMESTAMP(9), false);
    }

    @Test
    public void testTimestampToDateFallback() throws Exception {
        assertNestedEvolution(DataTypes.TIMESTAMP(3), DataTypes.DATE(), false);
        assertNestedEvolution(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.DATE(), false);
    }

    @Test
    public void testTimestampZoneChangeFallback() throws Exception {
        assertNestedEvolution(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), false);
        assertNestedEvolution(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP(3), false);
    }

    @Test
    public void testTimeToTimestampFallback() throws Exception {
        assertNestedEvolution(DataTypes.TIME(3), DataTypes.TIMESTAMP(3), false);
        assertNestedEvolution(DataTypes.TIME(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), false);
    }

    @Test
    public void testFormattedStringFallback() throws Exception {
        for (DataType source : Arrays.asList(DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.TIME(3),
                DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) {
            for (DataType target : Arrays.asList(DataTypes.STRING(), DataTypes.VARCHAR(30), DataTypes.CHAR(30))) {
                assertNestedEvolution(source, target, false);
            }
        }
    }

    @Test
    public void testConstructedStringFallback() throws Exception {
        for (DataType source : Arrays.asList(DataTypes.ROW(DataTypes.FIELD(2, "a", DataTypes.INT())),
                DataTypes.ARRAY(DataTypes.INT()), DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))) {
            assertNestedEvolution(source, DataTypes.STRING(), false);
        }
    }

    @Test
    public void testUnprovenScalarCastsFallback() throws Exception {
        assertNestedEvolution(DataTypes.STRING(), DataTypes.TIME(3), false);
        assertNestedEvolution(DataTypes.DATE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), false);
        assertNestedEvolution(DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP(3), false);
        assertNestedEvolution(DataTypes.VARCHAR(10), DataTypes.VARCHAR(3), false);
        assertNestedEvolution(DataTypes.CHAR(3), DataTypes.CHAR(10), false);
    }

    @Test
    public void testSafeEvolutionControls() throws Exception {
        DataType[][] pairs = {
                {DataTypes.TINYINT(), DataTypes.SMALLINT()},
                {DataTypes.INT(), DataTypes.BIGINT()},
                {DataTypes.INT(), DataTypes.DOUBLE()},
                {DataTypes.INT(), DataTypes.DECIMAL(12, 2)},
                {DataTypes.FLOAT(), DataTypes.DOUBLE()},
                {DataTypes.DECIMAL(10, 3), DataTypes.DECIMAL(10, 2)},
                {DataTypes.VARCHAR(10), DataTypes.VARCHAR(30)},
                {DataTypes.CHAR(3), DataTypes.VARCHAR(10)},
                {DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6)},
                {DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)},
                {DataTypes.DATE(), DataTypes.DATE()}
        };
        for (DataType[] pair : pairs) {
            assertNestedEvolution(pair[0], pair[1], true);
        }
    }

    @Test
    public void testNestedFieldsMatchById() throws Exception {
        DataType oldType = DataTypes.ROW(DataTypes.FIELD(2, "removed", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD(3, "old_name", DataTypes.INT()));
        DataType newType = DataTypes.ROW(DataTypes.FIELD(3, "renamed", DataTypes.BIGINT()),
                DataTypes.FIELD(4, "added", DataTypes.STRING()));
        assertNestedEvolution(oldType, newType, true);
    }

    @Test
    public void testHistoricalIntegerNarrowing() throws Exception {
        for (DataType target : Arrays.asList(DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT())) {
            assertNestedEvolution(DataTypes.BIGINT(), target, false);
        }
    }

    @Test
    public void testHistoricalStringsToTemporalTypes() throws Exception {
        for (DataType source : Arrays.asList(DataTypes.STRING(), DataTypes.VARCHAR(20), DataTypes.CHAR(20))) {
            for (DataType target : Arrays.asList(DataTypes.DATE(), DataTypes.TIMESTAMP(0), DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) {
                assertNestedEvolution(source, target, false);
            }
        }
    }

    @Test
    public void testHistoricalFloatingToDecimal() throws Exception {
        assertNestedEvolution(DataTypes.FLOAT(), DataTypes.DECIMAL(10, 2), false);
        assertNestedEvolution(DataTypes.DOUBLE(), DataTypes.DECIMAL(10, 2), false);
        assertNestedEvolution(DataTypes.INT(), DataTypes.DECIMAL(10, 2), true);
        assertNestedEvolution(DataTypes.DECIMAL(3, 2), DataTypes.DECIMAL(10, 2), true);
    }

    @Test
    public void testHistoricalNonIntegerToInteger() throws Exception {
        for (DataType source : Arrays.asList(DataTypes.FLOAT(), DataTypes.DOUBLE(),
                DataTypes.DECIMAL(10, 2), DataTypes.STRING())) {
            for (DataType target : Arrays.asList(DataTypes.TINYINT(), DataTypes.SMALLINT(),
                    DataTypes.INT(), DataTypes.BIGINT())) {
                assertNestedEvolution(source, target, false);
            }
        }
    }

    private void assertNestedEvolution(DataType source, DataType target, boolean expected) throws Exception {
        DataType[][] shapes = {
                {source, target},
                {DataTypes.ROW(new DataField(10, "old_name", source)),
                        DataTypes.ROW(new DataField(10, "renamed", target))},
                {DataTypes.ARRAY(source), DataTypes.ARRAY(target)},
                {DataTypes.MAP(source, DataTypes.INT()), DataTypes.MAP(target, DataTypes.INT())},
                {DataTypes.MAP(DataTypes.INT(), source), DataTypes.MAP(DataTypes.INT(), target)}
        };
        for (DataType[] shape : shapes) {
            Assertions.assertEquals(!containsHighPrecisionTimestamp(source),
                    canRead(shape[0], shape[0]), "Unchanged history: " + shape[0]);
            Assertions.assertEquals(expected, canRead(shape[0], shape[1]), Arrays.toString(shape));
        }
    }

    private static boolean containsHighPrecisionTimestamp(DataType type) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return org.apache.paimon.types.DataTypeChecks.getPrecision(type) > 6;
            default:
                return false;
        }
    }

    private boolean canRead(DataType source, DataType target) throws Exception {
        java.nio.file.Path directory = Files.createTempDirectory(temporaryFolder, "history");
        Files.createDirectories(directory.resolve("schema"));
        Files.write(directory.resolve("schema/schema-1"),
                PaimonScanPlanProvider.encodeTableSchema(schema(1, source)).getBytes(StandardCharsets.UTF_8));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), new Path(directory.toUri()),
                schema(2, target), CatalogEnvironment.empty());
        DataFileMeta file = DataFileMeta.forAppend("historical.parquet", 100L, 1L, SimpleStats.EMPTY_STATS,
                0L, 0L, 1L, Collections.emptyList(), null, FileSource.APPEND,
                Collections.emptyList(), null, null, Collections.emptyList());
        DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW).withBucket(0)
                .withBucketPath("file:///warehouse/table/bucket-0")
                .withDataFiles(Collections.singletonList(file)).build();
        return new PaimonRustReaderCapabilities(table, Collections.emptyList()).canRead(split);
    }

    private TableSchema schema(long id, DataType valueType) {
        return new TableSchema(id, Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "v", valueType)), 10, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null);
    }
}
