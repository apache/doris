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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.foundation.util.ArgumentParsers;

import com.google.common.collect.Lists;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Safely lists or deletes old files that are unreachable from every retained snapshot. */
public class IcebergRemoveOrphanFilesAction extends BaseIcebergAction {
    public static final String OLDER_THAN = "older_than";
    public static final String LOCATION = "location";
    public static final String DRY_RUN = "dry_run";

    public IcebergRemoveOrphanFilesAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("remove_orphan_files", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerRequiredArgument(OLDER_THAN, "Creation time cutoff in milliseconds",
                ArgumentParsers.nonNegativeLong(OLDER_THAN));
        namedArguments.registerOptionalArgument(LOCATION, "Prefix within the table location",
                null, ArgumentParsers.nonEmptyString(LOCATION));
        namedArguments.registerOptionalArgument(DRY_RUN, "Only count orphan files", true,
                ArgumentParsers.booleanValue(DRY_RUN));
    }

    @Override
    protected void validateIcebergAction() {
        validateNoPartitions();
        validateNoWhereCondition();
        String location = namedArguments.getString(LOCATION);
        if (location != null) {
            try {
                normalizeLocation(location);
            } catch (IllegalArgumentException e) {
                throw new DorisConnectorException("Invalid location URI: " + location, e);
            }
        }
    }

    @Override
    protected List<String> executeAction(Table table, ConnectorSession session) {
        if (!(table.io() instanceof SupportsPrefixOperations)) {
            throw new DorisConnectorException("remove_orphan_files requires FileIO prefix listing support");
        }
        String tableLocation = normalizeLocation(table.location());
        String scanLocation = namedArguments.getString(LOCATION);
        scanLocation = scanLocation == null ? tableLocation : normalizeLocation(scanLocation);
        // Normalize dot segments before the containment check so local FileIO paths cannot escape the table root.
        if (!scanLocation.equals(tableLocation) && !scanLocation.startsWith(tableLocation + "/")) {
            throw new DorisConnectorException("location must be within the Iceberg table location");
        }

        try {
            Set<String> reachable = collectReachableFiles(table);
            long orphanCount = 0;
            long deletedCount = 0;
            long olderThan = namedArguments.getLong(OLDER_THAN);
            boolean dryRun = namedArguments.getBoolean(DRY_RUN);
            // Object stores use raw prefix matching, so the separator prevents "table_backup" siblings
            // from being treated as children of "table".
            String listingPrefix = scanLocation.endsWith("/") ? scanLocation : scanLocation + "/";
            for (FileInfo file : ((SupportsPrefixOperations) table.io()).listPrefix(listingPrefix)) {
                if (file.createdAtMillis() < olderThan && !reachable.contains(file.location())) {
                    orphanCount++;
                    if (!dryRun) {
                        table.io().deleteFile(file.location());
                        deletedCount++;
                    }
                }
            }
            return Lists.newArrayList(String.valueOf(orphanCount), String.valueOf(deletedCount));
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to remove orphan files: " + e.getMessage(), e);
        }
    }

    private Set<String> collectReachableFiles(Table table) throws IOException {
        Set<String> reachable = new HashSet<>(ReachableFileUtil.metadataFileLocations(table, true));
        Set<String> scannedDeleteManifests = new HashSet<>();
        reachable.addAll(ReachableFileUtil.manifestListLocations(table));
        reachable.addAll(ReachableFileUtil.statisticsFilesLocations(table));
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                reachable.add(manifest.path());
            }
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                if (!scannedDeleteManifests.add(manifest.path())) {
                    continue;
                }
                // A retained delete file may not apply to any current data task, so read delete manifests directly.
                try (ManifestReader<DeleteFile> deletes =
                        ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                    deletes.forEach(delete -> reachable.add(delete.location()));
                }
            }
            try (CloseableIterable<FileScanTask> tasks = table.newScan()
                    .useSnapshot(snapshot.snapshotId()).planFiles()) {
                for (FileScanTask task : tasks) {
                    reachable.add(task.file().location());
                    task.deletes().forEach(delete -> reachable.add(delete.location()));
                }
            }
        }
        return reachable;
    }

    private String normalizeLocation(String location) {
        String normalized = URI.create(location).normalize().toString();
        return normalized.length() > 1 && normalized.endsWith("/")
                ? normalized.substring(0, normalized.length() - 1) : normalized;
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("orphan_file_count", ConnectorType.of("BIGINT"),
                        "Number of old unreachable files", false, null),
                new ConnectorColumn("deleted_file_count", ConnectorType.of("BIGINT"),
                        "Number of files deleted", false, null));
    }
}
