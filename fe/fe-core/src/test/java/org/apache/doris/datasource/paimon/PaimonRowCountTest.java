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

import org.apache.doris.catalog.TableIf;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class PaimonRowCountTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSystemTableEstimateDoesNotLoadMetadataOrPlan() {
        PaimonSysExternalTable table = Mockito.mock(PaimonSysExternalTable.class, invocation -> {
            throw new AssertionError("Row count estimation must not access metadata: " + invocation.getMethod());
        });
        Mockito.doCallRealMethod().when(table).fetchRowCount();

        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, table.fetchRowCount());
    }

    @Test
    public void testLatestCountWithoutManifests() throws Exception {
        for (boolean primaryKey : new boolean[] {false, true}) {
            FileStoreTable table = newTable(primaryKey);
            snapshot(table, 1, 10L);
            snapshot(table, 2, 25L);
            Assert.assertEquals(25L, rowCount(table));
        }
    }

    @Test
    public void testSnapshotTimestampAndTagCounts() throws Exception {
        FileStoreTable table = newTable(false);
        snapshot(table, 1, 10L);
        table.createTag("retained", 1L);
        snapshot(table, 2, 25L);
        Assert.assertEquals(10L, rowCount(table.copy(ImmutableMap.of("scan.snapshot-id", "1"))));
        Assert.assertEquals(10L, rowCount(table.copy(ImmutableMap.of("scan.timestamp-millis", "1500"))));
        table.fileIO().delete(table.snapshotManager().snapshotPath(1), false);
        Assert.assertEquals(10L, rowCount(table.copy(ImmutableMap.of("scan.tag-name", "retained"))));
    }

    @Test
    public void testEmptyAndNonpositiveCountsReturnUnknown() throws Exception {
        FileStoreTable table = newTable(false);
        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount(table));
        snapshot(table, 1, 0L);
        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount(table));
        snapshot(table, 2, -1L);
        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount(table));
    }

    @Test
    public void testPartialScansReturnUnknownWithoutManifests() throws Exception {
        FileStoreTable table = newTable(false);
        snapshot(table, 1, 10L);
        snapshot(table, 2, 25L);
        for (String[] option : new String[][] {
                {"incremental-between", "1,2"},
                {"scan.file-creation-time-millis", "1500"},
                {"scan.creation-time-millis", "1500"},
                {"scan.mode", "compacted-full"}}) {
            Assert.assertEquals(option[0], TableIf.UNKNOWN_ROW_COUNT,
                    rowCount(table.copy(ImmutableMap.of(option[0], option[1]))));
        }
        FileStoreTable deletionVectors = newTable(true, ImmutableMap.of("deletion-vectors.enabled", "true"));
        snapshot(deletionVectors, 1, 10L);
        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount(deletionVectors));
        FileStoreTable postponedBuckets = newTable(true, ImmutableMap.of("bucket", "-2"));
        snapshot(postponedBuckets, 1, 10L);
        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount(postponedBuckets));
    }

    @Test
    public void testUnsupportedTablesDoNotPlan() throws Exception {
        Table formatTable = Mockito.mock(Table.class);
        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, rowCount(formatTable));
        Mockito.verify(formatTable, Mockito.never()).newReadBuilder();
        FileStoreTable main = newTable(false);
        FileStoreTable other = newTable(false);
        snapshot(main, 1, 10L);
        snapshot(other, 1, 20L);
        Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT,
                rowCount(new FallbackReadFileStoreTable(main, other, true)));
    }

    @Test
    public void testCatalogQueryAuthorizationWithoutPlanning() throws Exception {
        FileStoreTable table = Mockito.spy(newTable(false).copy(ImmutableMap.of("query-auth.enabled", "true")));
        snapshot(table, 1, 10L);
        CatalogEnvironment environment = Mockito.mock(CatalogEnvironment.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.doReturn(environment).when(table).catalogEnvironment();
        Assert.assertEquals(10L, rowCount(table));
        Mockito.verify(environment.tableQueryAuth(Mockito.any(CoreOptions.class))).auth(null);
        Mockito.when(environment.tableQueryAuth(Mockito.any(CoreOptions.class)).auth(null))
                .thenThrow(new IllegalStateException("Query is not authorized"));
        Assert.assertThrows(IllegalStateException.class, () -> rowCount(table));
        Mockito.verify(table, Mockito.never()).newReadBuilder();
    }

    private long rowCount(Table table) {
        PaimonExternalTable external = Mockito.mock(PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doNothing().when(external).makeSureInitialized();
        Mockito.doReturn(table).when(external).getBasePaimonTable();
        return external.fetchRowCount();
    }

    private FileStoreTable newTable(boolean primaryKey) throws Exception {
        return newTable(primaryKey, Collections.emptyMap());
    }

    private FileStoreTable newTable(boolean primaryKey, Map<String, String> options) throws Exception {
        Path path = new Path(temporaryFolder.newFolder().toURI());
        LocalFileIO fileIO = LocalFileIO.create();
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT())
                .option("scan.manifest.parallelism", "1");
        if (primaryKey) {
            schema.primaryKey("id").option("bucket", "1");
        }
        options.forEach(schema::option);
        new FileSystemSchemaManager(fileIO, path).createTable(schema.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private void snapshot(FileStoreTable table, long id, long count) throws Exception {
        // No manifest files exist: accidentally returning to split planning must fail.
        Snapshot snapshot = new Snapshot(id, 0L, "unused-base", null, "unused-delta", null,
                null, null, null, "test", null, id, Snapshot.CommitKind.APPEND, id * 1000,
                count, count, null, null, null, Collections.emptyMap(), null, null);
        table.fileIO().mkdirs(table.snapshotManager().snapshotPath(id).getParent());
        table.fileIO().overwriteFileUtf8(table.snapshotManager().snapshotPath(id), snapshot.toJson());
        table.snapshotManager().commitLatestHint(id);
    }
}
