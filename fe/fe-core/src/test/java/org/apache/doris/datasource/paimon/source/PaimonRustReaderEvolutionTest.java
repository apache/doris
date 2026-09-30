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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class PaimonRustReaderEvolutionTest {
    @Test
    public void testPrimaryKeyMergeEnginesWithOneInsertOnlyFile() {
        for (String engine : Arrays.asList("deduplicate", "first-row", "partial-update", "aggregation")) {
            FileStoreTable table = Mockito.mock(FileStoreTable.class);
            TableSchema schema = new TableSchema(2, Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "v", DataTypes.DOUBLE())), 1, Collections.emptyList(),
                    Collections.singletonList("id"), Collections.singletonMap("merge-engine", engine), null);
            Mockito.when(table.schema()).thenReturn(schema);
            DataFileMeta file = Mockito.mock(DataFileMeta.class);
            Mockito.when(file.schemaId()).thenReturn(2L);
            Mockito.when(file.deleteRowCount()).thenReturn(Optional.of(0L));
            DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW).withBucket(0)
                    .withBucketPath("file:///warehouse/table/bucket-0")
                    .withDataFiles(Collections.singletonList(file)).build();
            // Older writers can keep repeated inserts inside one aggregate/partial-update file.
            Assert.assertEquals(engine, "deduplicate".equals(engine) || "first-row".equals(engine),
                    new PaimonRustReaderCapabilities(table, new TupleDescriptor(new TupleId(0))).canRead(split));
        }
    }

    @Test
    public void testTimestampEvolutionIntoNanosecondRangeFallback() {
        for (int oldPrecision : new int[] {0, 3, 6}) {
            for (int newPrecision : new int[] {7, 8, 9}) {
                assertNestedEvolution(DataTypes.TIMESTAMP(oldPrecision), DataTypes.TIMESTAMP(newPrecision), false);
                assertNestedEvolution(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(oldPrecision),
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(newPrecision), false);
            }
        }
        assertNestedEvolution(DataTypes.TIMESTAMP(7), DataTypes.TIMESTAMP(9), true);
    }

    @Test
    public void testTimestampToDateFallback() {
        assertNestedEvolution(DataTypes.TIMESTAMP(3), DataTypes.DATE(), false);
        assertNestedEvolution(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.DATE(), false);
    }

    @Test
    public void testTimestampZoneChangeFallback() {
        assertNestedEvolution(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), false);
        assertNestedEvolution(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP(3), false);
    }

    @Test
    public void testTimeToTimestampFallback() {
        assertNestedEvolution(DataTypes.TIME(3), DataTypes.TIMESTAMP(3), false);
        assertNestedEvolution(DataTypes.TIME(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), false);
    }

    @Test
    public void testFormattedStringFallback() {
        for (DataType source : Arrays.asList(DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.TIME(3),
                DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) {
            for (DataType target : Arrays.asList(DataTypes.STRING(), DataTypes.VARCHAR(30), DataTypes.CHAR(30))) {
                assertNestedEvolution(source, target, false);
            }
        }
    }

    @Test
    public void testConstructedStringFallback() {
        for (DataType source : Arrays.asList(DataTypes.ROW(DataTypes.FIELD(2, "a", DataTypes.INT())),
                DataTypes.ARRAY(DataTypes.INT()), DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))) {
            assertNestedEvolution(source, DataTypes.STRING(), false);
        }
    }

    @Test
    public void testUnprovenScalarCastsFallback() {
        assertNestedEvolution(DataTypes.STRING(), DataTypes.TIME(3), false);
        assertNestedEvolution(DataTypes.DATE(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), false);
        assertNestedEvolution(DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP(3), false);
        assertNestedEvolution(DataTypes.VARCHAR(10), DataTypes.VARCHAR(3), false);
        assertNestedEvolution(DataTypes.CHAR(3), DataTypes.CHAR(10), false);
    }

    @Test
    public void testSafeEvolutionControls() {
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
    public void testNestedFieldsMatchById() {
        DataType oldType = DataTypes.ROW(DataTypes.FIELD(2, "removed", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD(3, "old_name", DataTypes.INT()));
        DataType newType = DataTypes.ROW(DataTypes.FIELD(3, "renamed", DataTypes.BIGINT()),
                DataTypes.FIELD(4, "added", DataTypes.STRING()));
        assertNestedEvolution(oldType, newType, true);
    }

    private void assertNestedEvolution(DataType source, DataType target, boolean expected) {
        DataType[][] shapes = {
                {source, target},
                {DataTypes.ROW(new DataField(10, "old_name", source)),
                        DataTypes.ROW(new DataField(10, "renamed", target))},
                {DataTypes.ARRAY(source), DataTypes.ARRAY(target)},
                {DataTypes.MAP(source, DataTypes.INT()), DataTypes.MAP(target, DataTypes.INT())},
                {DataTypes.MAP(DataTypes.INT(), source), DataTypes.MAP(DataTypes.INT(), target)}
        };
        for (DataType[] shape : shapes) {
            Assert.assertTrue("Unchanged history must remain eligible: " + shape[0], canRead(shape[0], shape[0]));
            Assert.assertEquals(Arrays.toString(shape), expected, canRead(shape[0], shape[1]));
        }
    }

    private boolean canRead(DataType source, DataType target) {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        SchemaManager schemas = Mockito.mock(SchemaManager.class);
        Mockito.when(table.schema()).thenReturn(schema(2, target));
        Mockito.when(table.schemaManager()).thenReturn(schemas);
        Mockito.when(schemas.schema(1)).thenReturn(schema(1, source));
        DataFileMeta file = DataFileMeta.forAppend("historical.parquet", 100L, 1L, SimpleStats.EMPTY_STATS,
                0L, 0L, 1L, Collections.emptyList(), null, FileSource.APPEND,
                Collections.emptyList(), null, null, Collections.emptyList());
        DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW).withBucket(0)
                .withBucketPath("file:///warehouse/table/bucket-0")
                .withDataFiles(Collections.singletonList(file)).build();
        return new PaimonRustReaderCapabilities(table, new TupleDescriptor(new TupleId(0))).canRead(split);
    }

    private TableSchema schema(long id, DataType valueType) {
        return new TableSchema(id, Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "v", valueType)), 10, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null);
    }
}
