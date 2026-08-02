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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.util.PropertyUtil;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Safely lists or deletes old files that are unreachable from every retained snapshot. */
public class IcebergRemoveOrphanFilesAction extends BaseIcebergAction {
    private static final long MIN_RETENTION_MS = Duration.ofHours(24).toMillis();
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
        if (!PropertyUtil.propertyAsBoolean(table.properties(), TableProperties.GC_ENABLED,
                TableProperties.GC_ENABLED_DEFAULT)) {
            // A GC-disabled table may share files with another table, so no destructive scan is safe.
            throw new DorisConnectorException("Cannot remove orphan files: Iceberg GC is disabled");
        }
        List<String> scanLocations = resolveScanLocations(table);

        try {
            ReachableIndex reachable = new ReachableIndex(collectReachableFiles(table));
            long orphanCount = 0;
            long deletedCount = 0;
            long olderThan = namedArguments.getLong(OLDER_THAN);
            // The SQL procedure needs a retention fence because concurrent uploads are not reachable until commit.
            if (olderThan > System.currentTimeMillis() - MIN_RETENTION_MS) {
                throw new DorisConnectorException(
                        "older_than must retain at least 24 hours of files");
            }
            boolean dryRun = namedArguments.getBoolean(DRY_RUN);
            Set<String> visitedFiles = new HashSet<>();
            for (String scanLocation : scanLocations) {
                // Object stores use raw prefix matching, so the separator excludes sibling prefixes.
                String listingPrefix = scanLocation.endsWith("/") ? scanLocation : scanLocation + "/";
                for (FileInfo file : ((SupportsPrefixOperations) table.io()).listPrefix(listingPrefix)) {
                    if (visitedFiles.add(file.location()) && file.createdAtMillis() < olderThan
                            && !isReachable(file.location(), reachable)) {
                        orphanCount++;
                        if (!dryRun) {
                            table.io().deleteFile(file.location());
                            deletedCount++;
                        }
                    }
                }
            }
            return Lists.newArrayList(String.valueOf(orphanCount), String.valueOf(deletedCount));
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to remove orphan files: " + e.getMessage(), e);
        }
    }

    private List<String> resolveScanLocations(Table table) {
        String tableRoot = normalizeLocation(table.location());
        String dataRoot = normalizeLocation(resolveDataLocation(table, tableRoot));
        Set<String> ownedRoots = new LinkedHashSet<>();
        ownedRoots.add(tableRoot);
        ownedRoots.add(dataRoot);

        String requested = namedArguments.getString(LOCATION);
        if (requested != null) {
            String normalized = normalizeLocation(requested);
            boolean owned = ownedRoots.stream().anyMatch(root -> isWithin(normalized, root));
            if (!owned) {
                throw new DorisConnectorException(
                        "location must be within an Iceberg table-owned metadata or data location");
            }
            return Lists.newArrayList(normalized);
        }

        List<String> roots = new ArrayList<>();
        for (String candidate : ownedRoots) {
            // Avoid listing a nested default data directory twice when the table root already covers it.
            if (ownedRoots.stream().noneMatch(other -> !other.equals(candidate) && isWithin(candidate, other))) {
                roots.add(candidate);
            }
        }
        return roots;
    }

    private String resolveDataLocation(Table table, String tableRoot) {
        Map<String, String> properties = table.properties();
        String dataLocation = nonEmpty(properties.get(TableProperties.WRITE_DATA_LOCATION));
        if (dataLocation == null && Boolean.parseBoolean(properties.get(TableProperties.OBJECT_STORE_ENABLED))) {
            dataLocation = nonEmpty(properties.get(TableProperties.OBJECT_STORE_PATH));
        }
        if (dataLocation == null) {
            dataLocation = nonEmpty(properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION));
        }
        return dataLocation == null ? tableRoot + "/data" : dataLocation;
    }

    private String nonEmpty(String location) {
        return location == null || location.isEmpty() ? null : location;
    }

    private boolean isWithin(String location, String root) {
        FileIdentity child = FileIdentity.of(location);
        FileIdentity parent = FileIdentity.of(root);
        String pathPrefix = parent.path.endsWith("/") ? parent.path : parent.path + "/";
        return child.scheme.equals(parent.scheme) && child.authority.equals(parent.authority)
                && (child.path.equals(parent.path) || child.path.startsWith(pathPrefix));
    }

    private Set<String> collectReachableFiles(Table table) throws IOException {
        Set<String> reachable = new HashSet<>(ReachableFileUtil.metadataFileLocations(table, true));
        // Hadoop tables consult this live pointer even though it is not part of the metadata log.
        reachable.add(ReachableFileUtil.versionHintLocation(table));
        Set<String> scannedDataManifests = new HashSet<>();
        Set<String> scannedDeleteManifests = new HashSet<>();
        reachable.addAll(ReachableFileUtil.manifestListLocations(table));
        reachable.addAll(ReachableFileUtil.statisticsFilesLocations(table));
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                reachable.add(manifest.path());
                if (manifest.content() == ManifestContent.DATA) {
                    // Snapshots inherit manifests, so read each path once to keep work linear.
                    if (scannedDataManifests.add(manifest.path())) {
                        try (ManifestReader<DataFile> dataFiles =
                                ManifestFiles.read(manifest, table.io(), table.specs())) {
                            dataFiles.forEach(dataFile -> reachable.add(dataFile.location()));
                        }
                    }
                } else if (scannedDeleteManifests.add(manifest.path())) {
                    // A retained delete file may not apply to any current data task, so read it directly.
                    try (ManifestReader<DeleteFile> deletes =
                            ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                        deletes.forEach(delete -> reachable.add(delete.location()));
                    }
                }
            }
        }
        return reachable;
    }

    private static boolean isReachable(String candidate, ReachableIndex reachable) {
        FileIdentity candidateIdentity = FileIdentity.of(candidate);
        if (reachable.identities.contains(candidateIdentity)) {
            return true;
        }
        if (reachable.paths.contains(candidateIdentity.path)) {
            // A path collision across unknown providers/authorities cannot be classified safely.
            throw new DorisConnectorException(
                    "Cannot determine whether listed and reachable file locations are equivalent");
        }
        return false;
    }

    static boolean sameFileIdentity(String first, String second) {
        return FileIdentity.of(first).equals(FileIdentity.of(second));
    }

    static void verifyNoPrefixMismatch(String candidate, Set<String> reachable) {
        FileIdentity candidateIdentity = FileIdentity.of(candidate);
        for (String retained : reachable) {
            FileIdentity retainedIdentity = FileIdentity.of(retained);
            // Matching paths with different providers/authorities are ambiguous; deletion must fail closed.
            if (candidateIdentity.path.equals(retainedIdentity.path)
                    && !candidateIdentity.equals(retainedIdentity)) {
                throw new DorisConnectorException(
                        "Cannot determine whether listed and reachable file locations are equivalent");
            }
        }
    }

    private static final class FileIdentity {
        private final String scheme;
        private final String authority;
        private final String path;

        private FileIdentity(String scheme, String authority, String path) {
            this.scheme = scheme;
            this.authority = authority;
            this.path = path;
        }

        private static FileIdentity of(String location) {
            URI uri = URI.create(location).normalize();
            String scheme = uri.getScheme();
            scheme = scheme == null ? "" : scheme.toLowerCase(Locale.ROOT);
            if (scheme.equals("s3a") || scheme.equals("s3n")) {
                scheme = "s3";
            }
            String authority = uri.getAuthority();
            authority = authority == null ? "" : authority.toLowerCase(Locale.ROOT);
            String path = uri.getPath();
            return new FileIdentity(scheme, authority, path == null ? "" : path);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof FileIdentity)) {
                return false;
            }
            FileIdentity that = (FileIdentity) other;
            return scheme.equals(that.scheme) && authority.equals(that.authority)
                    && path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scheme, authority, path);
        }
    }

    private static final class ReachableIndex {
        private final Set<FileIdentity> identities = new HashSet<>();
        private final Set<String> paths = new HashSet<>();

        private ReachableIndex(Set<String> locations) {
            for (String location : locations) {
                FileIdentity identity = FileIdentity.of(location);
                identities.add(identity);
                paths.add(identity.path);
            }
        }
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
