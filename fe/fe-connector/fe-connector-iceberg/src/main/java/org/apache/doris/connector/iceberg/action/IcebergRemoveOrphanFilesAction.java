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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Safely lists or deletes old files that are unreachable from every retained snapshot. */
public class IcebergRemoveOrphanFilesAction extends BaseIcebergAction {
    private static final long MIN_RETENTION_MS = Duration.ofHours(24).toMillis();
    private static final int MAX_REACHABLE_FILES = 5_000_000;
    public static final String OLDER_THAN = "older_than";
    public static final String LOCATION = "location";
    public static final String DRY_RUN = "dry_run";
    public static final String ALLOW_UNSAFE_LOCATION = "allow_unsafe_location";

    public IcebergRemoveOrphanFilesAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("remove_orphan_files", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerRequiredArgument(OLDER_THAN, "Creation time cutoff in milliseconds",
                ArgumentParsers.nonNegativeLong(OLDER_THAN));
        namedArguments.registerOptionalArgument(LOCATION, "Prefix to scan for orphan files",
                null, ArgumentParsers.nonEmptyString(LOCATION));
        namedArguments.registerOptionalArgument(DRY_RUN, "Only count orphan files", true,
                ArgumentParsers.booleanValue(DRY_RUN));
        namedArguments.registerOptionalArgument(ALLOW_UNSAFE_LOCATION,
                "Allow an explicitly supplied location whose table ownership cannot be proved",
                false, ArgumentParsers.booleanValue(ALLOW_UNSAFE_LOCATION));
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
        long olderThan = namedArguments.getLong(OLDER_THAN);
        // Reject an unsafe cutoff before opening any metadata or manifest file.
        if (olderThan > System.currentTimeMillis() - MIN_RETENTION_MS) {
            throw new DorisConnectorException("older_than must retain at least 24 hours of files");
        }
        List<ScanScope> scanScopes = resolveScanScopes(table);

        try {
            ReachableIndex reachable = collectReachableFiles(table);
            long orphanCount = 0;
            long deletedCount = 0;
            boolean dryRun = namedArguments.getBoolean(DRY_RUN);
            for (ScanScope scope : scanScopes) {
                // Object stores use raw prefix matching, so the separator excludes sibling prefixes.
                String listingPrefix = scope.root.endsWith("/") ? scope.root : scope.root + "/";
                for (FileInfo file : ((SupportsPrefixOperations) table.io()).listPrefix(listingPrefix)) {
                    if (scope.owns(file.location()) && file.createdAtMillis() < olderThan
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

    private List<ScanScope> resolveScanScopes(Table table) {
        String tableRoot = normalizeLocation(table.location());
        String requested = namedArguments.getString(LOCATION);
        if (requested != null) {
            String normalized = normalizeLocation(requested);
            if (isWithin(normalized, tableRoot)) {
                return Lists.newArrayList(ScanScope.exclusive(normalized));
            }
            if (!namedArguments.getBoolean(ALLOW_UNSAFE_LOCATION)) {
                throw new DorisConnectorException(
                        "Cannot prove that location is owned by this table; set allow_unsafe_location=true "
                                + "only after verifying the prefix is exclusive to the table");
            }
            // This explicit escape hatch also covers historical roots after a table-location migration.
            return Lists.newArrayList(ScanScope.exclusive(normalized));
        }
        if (nonEmpty(table.properties().get(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)) != null) {
            throw new DorisConnectorException(
                    "remove_orphan_files cannot infer ownership for a custom write.location-provider.impl; "
                            + "provide location with allow_unsafe_location=true after verifying exclusivity");
        }
        String metadataRoot = nonEmpty(table.properties().get(TableProperties.WRITE_METADATA_LOCATION));
        if (metadataRoot != null && !isWithin(normalizeLocation(metadataRoot), tableRoot)) {
            throw new DorisConnectorException(
                    "Cannot prove that the configured external metadata location is table-exclusive; "
                            + "provide location with allow_unsafe_location=true after verifying exclusivity");
        }
        List<ScanScope> scopes = new ArrayList<>();
        scopes.add(ScanScope.exclusive(tableRoot));
        if (Boolean.parseBoolean(table.properties().get(TableProperties.OBJECT_STORE_ENABLED))) {
            // Match Iceberg's ObjectStoreLocationProvider precedence exactly.
            String objectRoot = nonEmpty(table.properties().get(TableProperties.WRITE_DATA_LOCATION));
            if (objectRoot == null) {
                objectRoot = nonEmpty(table.properties().get(TableProperties.OBJECT_STORE_PATH));
            }
            if (objectRoot == null) {
                objectRoot = nonEmpty(table.properties().get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION));
            }
            if (objectRoot != null) {
                String normalizedObjectRoot = normalizeLocation(objectRoot);
                if (!isWithin(normalizedObjectRoot, tableRoot)) {
                    // Iceberg's hashed path retains only a suffix of the table location; that suffix is not
                    // a globally unique ownership key when multiple catalogs share an object-store root.
                    throw new DorisConnectorException(
                            "Cannot prove that the configured object-store root is table-exclusive; "
                                    + "provide location with allow_unsafe_location=true after verifying exclusivity");
                }
            }
        } else {
            String externalDataRoot = nonEmpty(table.properties().get(TableProperties.WRITE_DATA_LOCATION));
            if (externalDataRoot == null) {
                externalDataRoot = nonEmpty(
                        table.properties().get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION));
            }
            if (externalDataRoot != null && !isWithin(normalizeLocation(externalDataRoot), tableRoot)) {
                throw new DorisConnectorException(
                        "Cannot prove that the configured external data location is table-exclusive; "
                                + "provide location with allow_unsafe_location=true after verifying exclusivity");
            }
        }
        return scopes;
    }

    private static String nonEmpty(String location) {
        return location == null || location.isEmpty() ? null : location;
    }

    private boolean isWithin(String location, String root) {
        return isWithinLocation(location, root);
    }

    private static boolean isWithinLocation(String location, String root) {
        FileIdentity child = FileIdentity.of(location);
        FileIdentity parent = FileIdentity.of(root);
        String pathPrefix = parent.path.endsWith("/") ? parent.path : parent.path + "/";
        return child.scheme.equals(parent.scheme) && child.authority.equals(parent.authority)
                && (child.path.equals(parent.path) || child.path.startsWith(pathPrefix));
    }

    private ReachableIndex collectReachableFiles(Table table) throws IOException {
        ReachableIndex reachable = new ReachableIndex(MAX_REACHABLE_FILES);
        reachable.addAll(ReachableFileUtil.metadataFileLocations(table, true));
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
        FileIdentity retainedIdentity = reachable.byPath.get(candidateIdentity.path);
        if (candidateIdentity.equals(retainedIdentity)) {
            return true;
        }
        if (retainedIdentity != null) {
            // A path collision across unknown providers/authorities cannot be classified safely.
            throw new DorisConnectorException(
                    "Cannot determine whether listed and reachable file locations are equivalent");
        }
        return false;
    }

    static boolean sameFileIdentity(String first, String second) {
        return FileIdentity.of(first).equals(FileIdentity.of(second));
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

    static void verifyReachableIndexLimit(Set<String> locations, int maxEntries) {
        ReachableIndex index = new ReachableIndex(maxEntries);
        index.addAll(locations);
    }

    private static final class ReachableIndex {
        private final Map<String, FileIdentity> byPath = new LinkedHashMap<>();
        private final int maxEntries;

        private ReachableIndex(int maxEntries) {
            this.maxEntries = maxEntries;
        }

        private void addAll(Iterable<String> locations) {
            locations.forEach(this::add);
        }

        private void add(String location) {
            FileIdentity identity = FileIdentity.of(location);
            FileIdentity existing = byPath.putIfAbsent(identity.path, identity);
            if (existing != null && !existing.equals(identity)) {
                throw new DorisConnectorException(
                        "Cannot determine whether reachable file locations are equivalent");
            }
            if (existing == null && byPath.size() > maxEntries) {
                throw new DorisConnectorException(
                        "Reachable file index exceeds the safe in-memory limit of " + maxEntries);
            }
        }
    }

    private static final class ScanScope {
        private final String root;

        private ScanScope(String root) {
            this.root = root;
        }

        private static ScanScope exclusive(String root) {
            return new ScanScope(root);
        }

        private boolean owns(String candidate) {
            return isWithinLocation(candidate, root);
        }
    }

    private static String normalizeLocation(String location) {
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
