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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.procedure.ConnectorProcedureResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IcebergRemoveOrphanFilesActionTest {
    private static final long MIN_RETENTION_MS = Duration.ofHours(24).toMillis();

    @Test
    public void gcDisabledPreventsDeletion(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("table"),
                Collections.singletonMap(TableProperties.GC_ENABLED, "false"));
        Path orphan = createOldFile(temp.resolve("table/data/orphan.parquet"));
        IcebergRemoveOrphanFilesAction action = action(System.currentTimeMillis() - MIN_RETENTION_MS, false);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(table, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(orphan));
    }

    @Test
    public void recentCutoffCannotRaceAnUncommittedWriter(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("table"), Collections.emptyMap());
        Set<String> manifestPaths = new HashSet<>();
        table.snapshots().forEach(snapshot -> snapshot.allManifests(table.io())
                .forEach(manifest -> manifestPaths.add(manifest.path())));
        RecordingFileIO recordingFileIO = new RecordingFileIO(table.io(), manifestPaths);
        Table recordingTable = new BaseTable(
                new StaticTableOperations(((HasTableOperations) table).operations().current(), recordingFileIO),
                table.name());
        Path uncommitted = createOldFile(temp.resolve("table/data/uncommitted.parquet"));
        IcebergRemoveOrphanFilesAction action = action(System.currentTimeMillis(), false);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(recordingTable, ActionTestTables.session("UTC")));
        Assertions.assertEquals(0, recordingFileIO.manifestOpenCount());
        Assertions.assertTrue(Files.exists(uncommitted));
    }

    @Test
    public void keepsVersionHintWhileDeletingAnOldOrphan(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("table"), Collections.emptyMap());
        Path orphan = createOldFile(temp.resolve("table/data/orphan.parquet"));
        Path versionHint = Path.of(java.net.URI.create(ReachableFileUtil.versionHintLocation(table)));
        Files.setLastModifiedTime(versionHint, FileTime.fromMillis(1));
        IcebergRemoveOrphanFilesAction action = action(System.currentTimeMillis() - MIN_RETENTION_MS, false);
        action.validate();

        ConnectorProcedureResult result = action.execute(table, ActionTestTables.session("UTC"));

        Assertions.assertEquals("1", result.getRows().get(0).get(0));
        Assertions.assertEquals("1", result.getRows().get(0).get(1));
        Assertions.assertFalse(Files.exists(orphan));
        Assertions.assertTrue(Files.exists(versionHint));
    }

    @Test
    public void treatsS3SchemeAliasesAsTheSameFile() {
        Assertions.assertTrue(IcebergRemoveOrphanFilesAction.sameFileIdentity(
                "s3://bucket/path/data.parquet", "s3a://bucket/path/data.parquet"));
        Assertions.assertTrue(IcebergRemoveOrphanFilesAction.sameFileIdentity(
                "s3n://bucket/path/data.parquet", "s3://BUCKET/path/data.parquet"));
    }

    @Test
    public void readsEachSharedDataManifestOnlyOnce(@TempDir Path temp) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.MANIFEST_MERGE_ENABLED, "false");
        Table table = createTable(temp.resolve("table"), properties);
        appendDataFile(table, createOldFile(temp.resolve("table/data/first.parquet")));
        appendDataFile(table, createOldFile(temp.resolve("table/data/second.parquet")));

        Set<String> dataManifestPaths = new HashSet<>();
        int[] manifestReferenceCount = {0};
        table.snapshots().forEach(snapshot -> snapshot.dataManifests(table.io())
                .forEach(manifest -> {
                    manifestReferenceCount[0]++;
                    dataManifestPaths.add(manifest.path());
                }));
        Assertions.assertTrue(manifestReferenceCount[0] > dataManifestPaths.size());
        RecordingFileIO recordingFileIO = new RecordingFileIO(table.io(), dataManifestPaths);
        Table recordingTable = new BaseTable(
                new StaticTableOperations(((HasTableOperations) table).operations().current(), recordingFileIO),
                table.name());
        IcebergRemoveOrphanFilesAction action = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, true);
        action.validate();

        action.execute(recordingTable, ActionTestTables.session("UTC"));

        Assertions.assertEquals(dataManifestPaths.size(), recordingFileIO.manifestOpenCount());
        dataManifestPaths.forEach(path -> Assertions.assertEquals(1,
                recordingFileIO.openCounts.getOrDefault(path, 0), path));
    }

    @Test
    public void rejectsUnprovenExternalDataRootByDefault(@TempDir Path temp) throws Exception {
        Path dataRoot = temp.resolve("owned-data");
        Table table = createTable(temp.resolve("metadata"),
                Collections.singletonMap(TableProperties.WRITE_DATA_LOCATION,
                        dataRoot.toUri().toString()));
        Path orphan = createOldFile(dataRoot.resolve("orphan.parquet"));
        IcebergRemoveOrphanFilesAction action = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(table, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(orphan));
    }

    @Test
    public void guardedExplicitDataRootDeletesButUnguardedArbitraryRootIsRejected(@TempDir Path temp)
            throws Exception {
        Path dataRoot = temp.resolve("owned-data");
        Table table = createTable(temp.resolve("metadata"),
                Collections.singletonMap(TableProperties.WRITE_DATA_LOCATION,
                        dataRoot.toUri().toString()));
        Path orphan = createOldFile(dataRoot.resolve("orphan.parquet"));

        IcebergRemoveOrphanFilesAction configured = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false, dataRoot.toUri().toString(), true);
        configured.validate();
        configured.execute(table, ActionTestTables.session("UTC"));
        Assertions.assertFalse(Files.exists(orphan));

        IcebergRemoveOrphanFilesAction arbitrary = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false,
                temp.resolve("unowned").toUri().toString());
        arbitrary.validate();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> arbitrary.execute(table, ActionTestTables.session("UTC")));
    }

    @Test
    public void guardedExplicitLocationCoversFormerTableRootAfterMigration(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("current-table"), Collections.emptyMap());
        Path sharedFormerRoot = temp.resolve("former-root-now-shared");
        Path formerDataRoot = sharedFormerRoot.resolve("old-table-data");
        Path formerMetadataRoot = sharedFormerRoot.resolve("old-table-metadata");
        Path dataOrphan = createOldFile(formerDataRoot.resolve("orphan.parquet"));
        Path metadataOrphan = createOldFile(formerMetadataRoot.resolve("orphan.metadata.json"));
        Path neighborFile = createOldFile(sharedFormerRoot.resolve("neighbor-table/live.parquet"));

        IcebergRemoveOrphanFilesAction unguarded = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false,
                sharedFormerRoot.toUri().toString());
        unguarded.validate();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> unguarded.execute(table, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(dataOrphan));
        Assertions.assertTrue(Files.exists(metadataOrphan));
        Assertions.assertTrue(Files.exists(neighborFile));

        IcebergRemoveOrphanFilesAction dataAction = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false,
                formerDataRoot.toUri().toString(), true);
        dataAction.validate();
        dataAction.execute(table, ActionTestTables.session("UTC"));
        IcebergRemoveOrphanFilesAction metadataAction = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false,
                formerMetadataRoot.toUri().toString(), true);
        metadataAction.validate();
        metadataAction.execute(table, ActionTestTables.session("UTC"));

        Assertions.assertFalse(Files.exists(dataOrphan));
        Assertions.assertFalse(Files.exists(metadataOrphan));
        Assertions.assertTrue(Files.exists(neighborFile));
    }

    @Test
    public void objectStoreOwnershipExcludesNeighborTableAndFolderRootFailsClosed(@TempDir Path temp)
            throws Exception {
        Path objectRoot = temp.resolve("object-data");
        Map<String, String> objectProperties = new HashMap<>();
        objectProperties.put(TableProperties.OBJECT_STORE_ENABLED, "true");
        objectProperties.put(TableProperties.WRITE_DATA_LOCATION, objectRoot.toUri().toString());
        objectProperties.put(TableProperties.OBJECT_STORE_PATH,
                temp.resolve("lower-precedence-object-path").toUri().toString());
        Files.createDirectories(objectRoot);
        Table objectTable = createTable(temp.resolve("object-metadata"), objectProperties);
        String ownLocation = objectTable.locationProvider().newDataLocation("own.parquet");
        Path ownOrphan = createOldFile(Path.of(java.net.URI.create(ownLocation)));
        Table neighborTable = createTable(temp.resolve("neighbor"), objectProperties);
        String neighborLocation = neighborTable.locationProvider().newDataLocation("live.parquet");
        Path neighborFile = createOldFile(Path.of(java.net.URI.create(neighborLocation)));
        Path folderRoot = temp.resolve("folder-data");
        Table folderTable = createTable(temp.resolve("folder-metadata"),
                Collections.singletonMap(TableProperties.WRITE_FOLDER_STORAGE_LOCATION,
                        folderRoot.toUri().toString()));
        Path folderOrphan = createOldFile(folderRoot.resolve("orphan.parquet"));

        IcebergRemoveOrphanFilesAction objectAction = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false);
        objectAction.validate();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> objectAction.execute(objectTable, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(ownOrphan));
        Assertions.assertTrue(Files.exists(neighborFile));

        IcebergRemoveOrphanFilesAction guardedObjectAction = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false,
                ownOrphan.getParent().toUri().toString(), true);
        guardedObjectAction.validate();
        guardedObjectAction.execute(objectTable, ActionTestTables.session("UTC"));
        Assertions.assertFalse(Files.exists(ownOrphan));
        Assertions.assertTrue(Files.exists(neighborFile));

        Map<String, String> prefixCollisionProperties = new HashMap<>();
        prefixCollisionProperties.put(TableProperties.OBJECT_STORE_ENABLED, "true");
        prefixCollisionProperties.put(TableProperties.WRITE_DATA_LOCATION,
                temp.resolve("prefix-table-shared").toUri().toString());
        Table prefixCollisionTable = createTable(temp.resolve("prefix-table"), prefixCollisionProperties);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> objectAction.execute(prefixCollisionTable, ActionTestTables.session("UTC")));

        IcebergRemoveOrphanFilesAction folderAction = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false);
        folderAction.validate();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> folderAction.execute(folderTable, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(folderOrphan));
    }

    @Test
    public void sharedObjectRootWithSameTableSuffixFailsClosed(@TempDir Path temp) throws Exception {
        Path sharedRoot = temp.resolve("shared-object-root");
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.OBJECT_STORE_ENABLED, "true");
        properties.put(TableProperties.WRITE_DATA_LOCATION, sharedRoot.toUri().toString());
        Table first = createTable(temp.resolve("catalog-a/db/t"), properties);
        Table second = createTable(temp.resolve("catalog-b/db/t"), properties);
        Path firstFile = createOldFile(Path.of(java.net.URI.create(
                first.locationProvider().newDataLocation("first.parquet"))));
        Path secondFile = createOldFile(Path.of(java.net.URI.create(
                second.locationProvider().newDataLocation("second.parquet"))));

        IcebergRemoveOrphanFilesAction action = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(first, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(firstFile));
        Assertions.assertTrue(Files.exists(secondFile));
    }

    @Test
    public void reachableIndexHasExplicitSafetyCap() {
        Set<String> files = Set.of("s3://bucket/table/a", "s3://bucket/table/b", "s3://bucket/table/c");
        Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergRemoveOrphanFilesAction.verifyReachableIndexLimit(files, 2));
    }

    @Test
    public void customLocationProviderRequiresGuardedExplicitLocation(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("table"), Collections.singletonMap(
                TableProperties.WRITE_LOCATION_PROVIDER_IMPL, "example.CustomProvider"));
        Path providerRoot = temp.resolve("provider-data");
        Path orphan = createOldFile(providerRoot.resolve("orphan.parquet"));
        IcebergRemoveOrphanFilesAction action = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, true);
        action.validate();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(table, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(orphan));

        IcebergRemoveOrphanFilesAction guarded = action(
                System.currentTimeMillis() - MIN_RETENTION_MS, false,
                providerRoot.toUri().toString(), true);
        guarded.validate();
        guarded.execute(table, ActionTestTables.session("UTC"));
        Assertions.assertFalse(Files.exists(orphan));
    }

    private static IcebergRemoveOrphanFilesAction action(long olderThan, boolean dryRun) {
        return action(olderThan, dryRun, null);
    }

    private static IcebergRemoveOrphanFilesAction action(long olderThan, boolean dryRun, String location) {
        return action(olderThan, dryRun, location, false);
    }

    private static IcebergRemoveOrphanFilesAction action(
            long olderThan, boolean dryRun, String location, boolean allowUnsafeLocation) {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergRemoveOrphanFilesAction.OLDER_THAN, String.valueOf(olderThan));
        properties.put(IcebergRemoveOrphanFilesAction.DRY_RUN, String.valueOf(dryRun));
        properties.put(IcebergRemoveOrphanFilesAction.ALLOW_UNSAFE_LOCATION,
                String.valueOf(allowUnsafeLocation));
        if (location != null) {
            properties.put(IcebergRemoveOrphanFilesAction.LOCATION, location);
        }
        return new IcebergRemoveOrphanFilesAction(properties, Collections.emptyList(), null);
    }

    private static Table createTable(Path location, Map<String, String> properties) {
        HadoopTables tables = new HadoopTables(new Configuration());
        return tables.create(ActionTestTables.SCHEMA, PartitionSpec.unpartitioned(), properties,
                location.toUri().toString());
    }

    private static Path createOldFile(Path path) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[] {1});
        Files.setLastModifiedTime(path, FileTime.fromMillis(1));
        return path;
    }

    private static void appendDataFile(Table table, Path path) {
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(path.toUri().toString())
                .withFileSizeInBytes(1)
                .withRecordCount(1)
                .build();
        table.newFastAppend().appendFile(dataFile).commit();
    }

    private static final class RecordingFileIO implements SupportsPrefixOperations {
        private final FileIO delegate;
        private final SupportsPrefixOperations prefixDelegate;
        private final Set<String> manifestPaths;
        private final Map<String, Integer> openCounts = new HashMap<>();

        private RecordingFileIO(FileIO delegate, Set<String> manifestPaths) {
            this.delegate = delegate;
            this.prefixDelegate = (SupportsPrefixOperations) delegate;
            this.manifestPaths = manifestPaths;
        }

        private void record(String path) {
            if (manifestPaths.contains(path)) {
                openCounts.merge(path, 1, Integer::sum);
            }
        }

        private int manifestOpenCount() {
            return openCounts.values().stream().mapToInt(Integer::intValue).sum();
        }

        @Override
        public InputFile newInputFile(String path) {
            record(path);
            return delegate.newInputFile(path);
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            record(path);
            return delegate.newInputFile(path, length);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
        }

        @Override
        public Map<String, String> properties() {
            return delegate.properties();
        }

        @Override
        public Iterable<FileInfo> listPrefix(String prefix) {
            return prefixDelegate.listPrefix(prefix);
        }

        @Override
        public void deletePrefix(String prefix) {
            prefixDelegate.deletePrefix(prefix);
        }
    }
}
