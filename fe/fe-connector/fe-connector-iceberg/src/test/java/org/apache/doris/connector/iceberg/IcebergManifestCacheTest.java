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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link IcebergManifestCache} (T08). Uses a real {@link InMemoryCatalog} table so the cache is
 * exercised against genuine iceberg {@link ManifestFile}s (no I/O — InMemoryCatalog serves manifests in-memory).
 */
public class IcebergManifestCacheTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static Table tableWithTwoDataFiles() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/f1.parquet").withFileSizeInBytes(100).withRecordCount(1).build())
                .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/f2.parquet").withFileSizeInBytes(200).withRecordCount(2).build())
                .commit();
        return table;
    }

    @Test
    public void loadsDataFilesAndCachesByManifestPath() {
        Table table = tableWithTwoDataFiles();
        List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
        Assertions.assertEquals(1, manifests.size(), "the single append produced one data manifest");
        ManifestFile manifest = manifests.get(0);

        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue first = cache.getManifestCacheValue(manifest, table);
        // WHY: a DATA manifest yields the two appended data files. MUTATION: returning the delete-file branch
        // (empty) -> 0 -> red.
        Assertions.assertEquals(2, first.getDataFiles().size());
        Assertions.assertTrue(first.getDeleteFiles().isEmpty());
        Assertions.assertEquals(1, cache.size());

        // A second read of the SAME manifest path returns the SAME cached value (no re-parse). MUTATION: not
        // caching (re-loading) -> a different instance -> red.
        ManifestCacheValue second = cache.getManifestCacheValue(manifest, table);
        Assertions.assertSame(first, second, "the manifest payload must be served from the cache on a repeat");
        Assertions.assertEquals(1, cache.size());
    }

    @Test
    public void capacityOverflowFlushesWholesale() {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        // maxSize 1: the first load fills it; a re-read still hits (same key). The wholesale-flush valve is the
        // legacy behavior — re-reads are harmless since the value is immutable. Smoke that size stays bounded.
        IcebergManifestCache cache = new IcebergManifestCache(1);
        cache.getManifestCacheValue(manifest, table);
        Assertions.assertEquals(1, cache.size());
        cache.getManifestCacheValue(manifest, table);
        Assertions.assertTrue(cache.size() <= 1, "a bounded cache must not grow past its max size");
    }

    @Test
    public void invalidateAllClearsEveryEntry() {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        cache.getManifestCacheValue(manifest, table);
        Assertions.assertEquals(1, cache.size());
        // REFRESH CATALOG hook (H-5): invalidateAll drops every cached manifest (legacy catalog-wide
        // group.invalidateAll parity). MUTATION: a no-op invalidateAll -> size stays 1 -> red.
        cache.invalidateAll();
        Assertions.assertEquals(0, cache.size());
    }

    @Test
    public void copiesDataFilesSoReaderReuseDoesNotAlias() {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        List<DataFile> files = new IcebergManifestCache().getManifestCacheValue(manifest, table).getDataFiles();
        // WHY: ManifestReader reuses one object across iterations, so the cache must .copy() each file. Two
        // distinct paths prove the entries were copied out (not the same reused instance). MUTATION: dropping
        // .copy() -> both entries alias the last file -> both paths equal -> red.
        Assertions.assertNotEquals(files.get(0).path().toString(), files.get(1).path().toString());
    }
}
