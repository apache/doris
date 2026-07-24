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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Removes old snapshots from an iceberg table to free storage and improve metadata performance. Connector
 * port of legacy {@code IcebergExpireSnapshotsAction} — the most involved of the snapshot procedures (five
 * optional arguments, a custom validation pass, an FE-local fixed thread pool for concurrent deletes, a
 * {@code deleteWith} callback that classifies deleted files into the six Spark-compatible counters, and the
 * delete-file content map). The validation messages and the SDK call chain are verbatim; the validation
 * failures throw {@link DorisConnectorException} in place of the legacy {@code AnalysisException}/
 * {@code UserException} (message-identical, T08 byte-parity). {@code parseTimestamp} keeps the legacy
 * {@code ZoneId.systemDefault()} (this procedure, unlike {@code rollback_to_timestamp}, never used the
 * session time zone).
 */
public class IcebergExpireSnapshotsAction extends BaseIcebergAction {
    private static final Logger LOG = LogManager.getLogger(IcebergExpireSnapshotsAction.class);
    public static final String OLDER_THAN = "older_than";
    public static final String RETAIN_LAST = "retain_last";
    public static final String MAX_CONCURRENT_DELETES = "max_concurrent_deletes";
    public static final String SNAPSHOT_IDS = "snapshot_ids";
    public static final String CLEAN_EXPIRED_METADATA = "clean_expired_metadata";

    // Test-only gate for the delete-manifest dedup: the number of DISTINCT delete manifests read by the most
    // recent buildDeleteFileContentMap call. Asserts each manifest is read once, not once per referencing snapshot.
    @VisibleForTesting
    int lastDeleteManifestReadCount;

    public IcebergExpireSnapshotsAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("expire_snapshots", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register optional arguments for expire_snapshots
        namedArguments.registerOptionalArgument(OLDER_THAN,
                "Timestamp before which snapshots will be removed",
                null, ArgumentParsers.nonEmptyString(OLDER_THAN));
        namedArguments.registerOptionalArgument(RETAIN_LAST,
                "Number of ancestor snapshots to preserve regardless of older_than",
                null, ArgumentParsers.positiveInt(RETAIN_LAST));
        namedArguments.registerOptionalArgument(MAX_CONCURRENT_DELETES,
                "Size of the thread pool used for delete file actions (0 disables, "
                        + "ignored for FileIOs that support bulk deletes)",
                0, ArgumentParsers.intRange(MAX_CONCURRENT_DELETES, 0, Integer.MAX_VALUE));
        namedArguments.registerOptionalArgument(SNAPSHOT_IDS,
                "Array of snapshot IDs to expire",
                null, ArgumentParsers.nonEmptyString(SNAPSHOT_IDS));
        namedArguments.registerOptionalArgument(CLEAN_EXPIRED_METADATA,
                "When true, cleans up metadata such as partition specs and schemas",
                null, ArgumentParsers.booleanValue(CLEAN_EXPIRED_METADATA));
    }

    @Override
    protected void validateIcebergAction() {
        // Validate older_than parameter (timestamp)
        String olderThan = namedArguments.getString(OLDER_THAN);
        if (olderThan != null) {
            try {
                // Try to parse as ISO datetime format
                LocalDateTime.parse(olderThan, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (DateTimeParseException e) {
                try {
                    // Try to parse as timestamp (milliseconds since epoch)
                    long timestamp = Long.parseLong(olderThan);
                    if (timestamp < 0) {
                        throw new DorisConnectorException("older_than timestamp must be non-negative");
                    }
                } catch (NumberFormatException nfe) {
                    throw new DorisConnectorException("Invalid older_than format. Expected ISO datetime "
                            + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: " + olderThan);
                }
            }
        }

        // Validate retain_last parameter
        Integer retainLast = namedArguments.getInt(RETAIN_LAST);
        if (retainLast != null && retainLast < 1) {
            throw new DorisConnectorException("retain_last must be at least 1");
        }

        // Get snapshot_ids for validation
        String snapshotIds = namedArguments.getString(SNAPSHOT_IDS);

        // Validate snapshot_ids format if provided
        if (snapshotIds != null) {
            for (String idStr : snapshotIds.split(",")) {
                try {
                    Long.parseLong(idStr.trim());
                } catch (NumberFormatException e) {
                    throw new DorisConnectorException("Invalid snapshot_id format: " + idStr.trim());
                }
            }
        }

        // At least one of older_than, retain_last, or snapshot_ids must be specified
        if (olderThan == null && retainLast == null && snapshotIds == null) {
            throw new DorisConnectorException("At least one of 'older_than', 'retain_last', or "
                    + "'snapshot_ids' must be specified");
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
        // Parse parameters
        String olderThan = namedArguments.getString(OLDER_THAN);
        Integer retainLast = namedArguments.getInt(RETAIN_LAST);
        String snapshotIdsStr = namedArguments.getString(SNAPSHOT_IDS);
        Boolean cleanExpiredMetadata = namedArguments.getBoolean(CLEAN_EXPIRED_METADATA);
        Integer maxConcurrentDeletes = namedArguments.getInt(MAX_CONCURRENT_DELETES);

        // Track deleted file counts using callbacks (matching Spark's 6-column schema)
        AtomicLong deletedDataFilesCount = new AtomicLong(0);
        AtomicLong deletedPositionDeleteFilesCount = new AtomicLong(0);
        AtomicLong deletedEqualityDeleteFilesCount = new AtomicLong(0);
        AtomicLong deletedManifestFilesCount = new AtomicLong(0);
        AtomicLong deletedManifestListsCount = new AtomicLong(0);
        AtomicLong deletedStatisticsFilesCount = new AtomicLong(0);

        ExecutorService deleteExecutor = null;
        try {
            Map<String, FileContent> deleteFileContentByPath =
                    buildDeleteFileContentMap(icebergTable);
            ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();

            // Configure older_than timestamp
            // If retain_last is specified without older_than, use current time as the cutoff
            // This is because Iceberg's retainLast only works in conjunction with expireOlderThan
            if (olderThan != null) {
                long timestampMillis = parseTimestamp(olderThan);
                expireSnapshots.expireOlderThan(timestampMillis);
            } else if (retainLast != null && snapshotIdsStr == null) {
                // When only retain_last is specified, expire all snapshots older than now
                // but keep at least retain_last snapshots
                expireSnapshots.expireOlderThan(System.currentTimeMillis());
            }

            // Configure retain_last
            if (retainLast != null) {
                expireSnapshots.retainLast(retainLast);
            }

            // Configure specific snapshot IDs to expire
            if (snapshotIdsStr != null) {
                for (String idStr : snapshotIdsStr.split(",")) {
                    expireSnapshots.expireSnapshotId(Long.parseLong(idStr.trim()));
                }
            }

            // Configure clean expired metadata
            if (cleanExpiredMetadata != null) {
                expireSnapshots.cleanExpiredMetadata(cleanExpiredMetadata);
            }

            // Set up ExecutorService for concurrent deletes if specified
            if (maxConcurrentDeletes > 0) {
                if (icebergTable.io() instanceof SupportsBulkOperations) {
                    LOG.warn("max_concurrent_deletes only works with FileIOs that do not support "
                            + "bulk deletes. This table is currently using {} which supports bulk deletes "
                            + "so the parameter will be ignored.",
                            icebergTable.io().getClass().getName());
                } else {
                    deleteExecutor = Executors.newFixedThreadPool(maxConcurrentDeletes);
                    expireSnapshots.executeDeleteWith(deleteExecutor);
                }
            }

            // Set up delete callback to count files by type
            expireSnapshots.deleteWith(path -> {
                FileContent deleteContent = deleteFileContentByPath.get(path);
                if (deleteContent == FileContent.POSITION_DELETES) {
                    deletedPositionDeleteFilesCount.incrementAndGet();
                } else if (deleteContent == FileContent.EQUALITY_DELETES) {
                    deletedEqualityDeleteFilesCount.incrementAndGet();
                } else if (path.contains("-m-") && path.endsWith(".avro")) {
                    deletedManifestFilesCount.incrementAndGet();
                } else if (path.contains("snap-") && path.endsWith(".avro")) {
                    deletedManifestListsCount.incrementAndGet();
                } else if (path.endsWith(".stats") || path.contains("statistics")) {
                    deletedStatisticsFilesCount.incrementAndGet();
                } else {
                    deletedDataFilesCount.incrementAndGet();
                }
                icebergTable.io().deleteFile(path);
            });

            // Execute and commit
            expireSnapshots.commit();

            return Lists.newArrayList(
                String.valueOf(deletedDataFilesCount.get()),
                String.valueOf(deletedPositionDeleteFilesCount.get()),
                String.valueOf(deletedEqualityDeleteFilesCount.get()),
                String.valueOf(deletedManifestFilesCount.get()),
                String.valueOf(deletedManifestListsCount.get()),
                String.valueOf(deletedStatisticsFilesCount.get())
            );
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to expire snapshots: " + e.getMessage(), e);
        } finally {
            // Shutdown executor if created
            if (deleteExecutor != null) {
                deleteExecutor.shutdown();
            }
        }
    }

    /**
     * Parse timestamp string to milliseconds since epoch.
     * Supports ISO datetime format (yyyy-MM-ddTHH:mm:ss) or milliseconds.
     */
    private long parseTimestamp(String timestamp) {
        try {
            // Try ISO datetime format
            LocalDateTime dateTime = LocalDateTime.parse(timestamp,
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return dateTime.atZone(ZoneId.systemDefault())
                .toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            // Try as milliseconds
            return Long.parseLong(timestamp);
        }
    }

    @VisibleForTesting
    Map<String, FileContent> buildDeleteFileContentMap(Table icebergTable) {
        Map<String, FileContent> deleteFileContentByPath = new HashMap<>();
        // Dedup delete-manifest reads across snapshots. Iceberg manifests are immutable and adjacent snapshots
        // carry the same delete manifests forward unchanged, so re-reading one yields the identical DeleteFile
        // set (putIfAbsent already made the re-read a no-op). Reading each DISTINCT manifest exactly once
        // collapses the O(snapshots x manifests) remote reads to O(distinct manifests) with a byte-identical map.
        // NOTE: visited MUST live at method scope (outside the snapshot loop) — inside it, it would reset every
        // snapshot and defeat the cross-snapshot dedup this fix targets.
        Set<String> visitedDeleteManifests = new HashSet<>();
        int reads = 0;
        try {
            for (Snapshot snapshot : icebergTable.snapshots()) {
                List<ManifestFile> deleteManifests = snapshot.deleteManifests(icebergTable.io());
                if (deleteManifests == null || deleteManifests.isEmpty()) {
                    continue;
                }
                for (ManifestFile manifest : deleteManifests) {
                    if (!visitedDeleteManifests.add(manifest.path())) {
                        continue;
                    }
                    reads++;
                    try (CloseableIterable<DeleteFile> deleteFiles = ManifestFiles.readDeleteManifest(
                            manifest, icebergTable.io(), icebergTable.specs())) {
                        for (DeleteFile deleteFile : deleteFiles) {
                            deleteFileContentByPath.putIfAbsent(
                                    deleteFile.location(), deleteFile.content());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to build delete file content map: " + e.getMessage(), e);
        }
        lastDeleteManifestReadCount = reads;
        return deleteFileContentByPath;
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
            new ConnectorColumn("deleted_data_files_count", ConnectorType.of("BIGINT"),
                "Number of data files deleted", false, null),
            new ConnectorColumn("deleted_position_delete_files_count", ConnectorType.of("BIGINT"),
                "Number of position delete files deleted", false, null),
            new ConnectorColumn("deleted_equality_delete_files_count", ConnectorType.of("BIGINT"),
                "Number of equality delete files deleted", false, null),
            new ConnectorColumn("deleted_manifest_files_count", ConnectorType.of("BIGINT"),
                "Number of manifest files deleted", false, null),
            new ConnectorColumn("deleted_manifest_lists_count", ConnectorType.of("BIGINT"),
                "Number of manifest list files deleted", false, null),
            new ConnectorColumn("deleted_statistics_files_count", ConnectorType.of("BIGINT"),
                "Number of statistics files deleted", false, null)
        );
    }
}
