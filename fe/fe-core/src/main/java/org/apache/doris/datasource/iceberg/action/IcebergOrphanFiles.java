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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.actions.FileURI;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.util.FileSystemWalker;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** Invocation-local implementation of the Iceberg 1.11 Spark orphan-file matching algorithm. */
final class IcebergOrphanFiles {
    private static final Logger LOG = LogManager.getLogger(IcebergOrphanFiles.class);
    private static final int DELETE_GROUP_SIZE = 100_000;
    private static final int MAX_SAMPLE_SIZE = 20_000;
    private final Table table;
    private final Configuration conf;
    private final String location;
    private final long olderThan;
    private final Map<String, String> schemes;
    private final Map<String, String> authorities;
    private final PrefixMismatchMode mode;
    private final Runnable checkCancelled;
    private final ExecutionAuthenticator authenticator;
    private final java.nio.file.Path tempDirectory;

    IcebergOrphanFiles(Table table, Configuration conf, String location, long olderThan,
            Map<String, String> schemes, Map<String, String> authorities, PrefixMismatchMode mode,
            Runnable checkCancelled, ExecutionAuthenticator authenticator, java.nio.file.Path tempDirectory) {
        this.table = table;
        this.conf = conf;
        this.location = location;
        this.olderThan = olderThan;
        this.schemes = new HashMap<>(schemes);
        this.authorities = new HashMap<>(authorities);
        this.mode = mode;
        this.checkCancelled = checkCancelled;
        this.authenticator = authenticator;
        this.tempDirectory = tempDirectory;
    }

    List<List<String>> execute(Consumer<Consumer<String>> fileList, boolean prefixListing,
            boolean dryRun, boolean streamResults, Integer concurrency) throws IOException {
        checkCancelled.run();
        ValidationException.check(PropertyUtil.propertyAsBoolean(table.properties(), TableProperties.GC_ENABLED,
                        TableProperties.GC_ENABLED_DEFAULT),
                "Cannot delete orphan files: GC is disabled (deleting files may corrupt other tables)");
        boolean bulk = table.io() instanceof SupportsBulkOperations;
        if (concurrency != null && bulk) {
            LOG.warn("max_concurrent_deletes only works with FileIOs that do not support bulk deletes. "
                    + "The parameter is ignored for {}", table.io().getClass().getName());
        }
        try (Candidates candidates = new Candidates(tempDirectory)) {
            identify(fileList, prefixListing, candidates);
            // The complete source and all prefix conflicts have been checked before the first delete.
            checkCancelled.run();
            return delete(candidates, dryRun, streamResults, bulk, concurrency);
        }
    }

    private void identify(Consumer<Consumer<String>> fileList, boolean prefixListing, Candidates candidates)
            throws IOException {
        Map<String, List<FileURI>> references = references();
        Set<Pair<String, String>> conflicts = new LinkedHashSet<>();
        Consumer<String> actual = path -> {
            checkCancelled.run();
            FileURI file = uri(path);
            List<FileURI> matches = references.get(file.getPath());
            if (matches == null) {
                candidates.add(path);
                return;
            }
            // Match the upstream left outer join, including multiplicity. An anti join or a
            // global DISTINCT would change DELETE-mode output for mixed prefixes/duplicate rows.
            for (FileURI valid : matches) {
                boolean schemeMatches = valid.schemeMatch(file);
                boolean authorityMatches = valid.authorityMatch(file);
                if ((!schemeMatches || !authorityMatches) && mode == PrefixMismatchMode.DELETE) {
                    candidates.add(path);
                } else if (mode == PrefixMismatchMode.ERROR) {
                    if (!schemeMatches) {
                        conflicts.add(Pair.of(valid.getScheme(), file.getScheme()));
                    }
                    if (!authorityMatches) {
                        conflicts.add(Pair.of(valid.getAuthority(), file.getAuthority()));
                    }
                }
            }
        };
        if (fileList != null) {
            fileList.accept(actual);
        } else {
            listFiles(prefixListing, actual);
        }
        ValidationException.check(conflicts.isEmpty(), "Unable to determine whether certain files are orphan. "
                + "Metadata references files that match listed/provided files except for authority/scheme. "
                + "Configure equal_schemes/equal_authorities or prefix_mismatch_mode. Conflicting prefixes: %s",
                conflicts);
        candidates.finish();
    }

    private Map<String, List<FileURI>> references() throws IOException {
        Map<String, List<FileURI>> references = new HashMap<>();
        Consumer<String> add = path -> {
            checkCancelled.run();
            FileURI file = uri(path);
            references.computeIfAbsent(file.getPath(), ignored -> new ArrayList<>()).add(file);
        };
        Set<String> readManifests = new HashSet<>();
        List<String> columns = Arrays.asList("file_path", "content");
        for (Snapshot snapshot : table.snapshots()) {
            checkCancelled.run();
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                // Spark manifestDS keeps each ALL_MANIFESTS row, whereas contentFileDS reads
                // each manifest path only once. Preserve both rules, including their duplicates.
                add.accept(manifest.path());
                if (readManifests.add(manifest.path())) {
                    try (CloseableIterable<? extends ContentFile<?>> files = manifest.content() == ManifestContent.DATA
                            ? ManifestFiles.read(manifest, table.io(), table.specs()).select(columns)
                            : ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs()).select(columns)) {
                        for (ContentFile<?> file : files) {
                            add.accept(file.location());
                        }
                    }
                }
            }
        }
        ReachableFileUtil.manifestListLocations(table).forEach(add);
        ReachableFileUtil.metadataFileLocations(table, false).forEach(add);
        add.accept(ReachableFileUtil.versionHintLocation(table));
        ReachableFileUtil.statisticsFilesLocations(table).forEach(add);
        return references;
    }

    private FileURI uri(String path) {
        // Hadoop Path is also used by Spark's ToFileURI; keep the original path for output/deletion.
        java.net.URI uri = new Path(path).toUri();
        return new FileURI(schemes.getOrDefault(uri.getScheme(), uri.getScheme()),
                authorities.getOrDefault(uri.getAuthority(), uri.getAuthority()), uri.getPath(), path);
    }

    private void listFiles(boolean prefixListing, Consumer<String> actual) {
        if (prefixListing) {
            ValidationException.check(table.io() instanceof SupportsPrefixOperations,
                    "Cannot use prefix listing with FileIO %s which does not support prefix operations",
                    table.io().getClass().getName());
            FileSystemWalker.listDirRecursivelyWithFileIO((SupportsPrefixOperations) table.io(), location,
                    table.specs(), info -> {
                        checkCancelled.run();
                        return info.createdAtMillis() < olderThan;
                    }, actual);
        } else {
            Predicate<FileStatus> older = file -> {
                checkCancelled.run();
                return file.getModificationTime() < olderThan;
            };
            List<String> pending = new ArrayList<>();
            FileSystemWalker.listDirRecursivelyWithHadoop(location, table.specs(), older, conf, 3, 10,
                    dir -> {
                        checkCancelled.run();
                        pending.add(dir);
                    }, actual);
            for (String dir : pending) {
                checkCancelled.run();
                List<String> tooDeep = new ArrayList<>();
                FileSystemWalker.listDirRecursivelyWithHadoop(dir, table.specs(), older, conf, 2000,
                        Integer.MAX_VALUE, tooDeep::add, actual);
                ValidationException.check(tooDeep.isEmpty(), "Could not list sub directories, "
                        + "reached maximum depth: 2000");
            }
        }
    }

    private List<List<String>> delete(Candidates candidates, boolean dryRun, boolean stream,
            boolean bulk, Integer concurrency) throws IOException {
        List<List<String>> result = new ArrayList<>();
        // Like Spark collectAsList, materialize a non-streaming result before any deletion.
        if (!stream) {
            try (DataInputStream input = candidates.open()) {
                for (long i = 0; i < candidates.count; i++) {
                    checkCancelled.run();
                    result.add(Collections.singletonList(Candidates.read(input)));
                }
            }
        }
        ExecutorService executor = concurrency == null || bulk || dryRun ? null : Executors.newFixedThreadPool(
                concurrency, new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("iceberg-orphan-delete-%d").build());
        try (DataInputStream input = candidates.open()) {
            long remaining = candidates.count;
            while (remaining > 0) {
                checkCancelled.run();
                int size = (int) Math.min(remaining, DELETE_GROUP_SIZE);
                List<String> paths = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    checkCancelled.run();
                    String path = Candidates.read(input);
                    paths.add(path);
                    if (stream && result.size() < MAX_SAMPLE_SIZE) {
                        result.add(Collections.singletonList(path));
                    }
                }
                checkCancelled.run();
                if (!dryRun) {
                    if (bulk) {
                        try {
                            ((SupportsBulkOperations) table.io()).deleteFiles(paths);
                        } catch (BulkDeletionFailureException e) {
                            LOG.warn("Deleted only {} of {} orphan candidates using bulk deletes",
                                    paths.size() - e.numberFailedObjects(), paths.size(), e);
                        }
                    } else {
                        Tasks.foreach(paths).noRetry().executeWith(executor).suppressFailureWhenFinished()
                                .onFailure((path, error) -> LOG.warn("Failed to delete orphan file: {}", path, error))
                                .run(this::deleteFile);
                    }
                }
                checkCancelled.run();
                remaining -= size;
            }
        } finally {
            if (executor != null) {
                // Tasks waits for submitted work. Do not use shutdownNow: discarded queued
                // Futures would otherwise remain incomplete in Iceberg Tasks' wait loop.
                executor.shutdown();
            }
        }
        LOG.info("Identified {} orphan candidates (dry_run={}, stream_results={})",
                candidates.count, dryRun, stream);
        return result;
    }

    private void deleteFile(String path) {
        try {
            authenticator.execute(() -> {
                checkCancelled.run();
                table.io().deleteFile(path);
                return null;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** A private, single-invocation spool; no catalog entry or persistent job state is created. */
    private static final class Candidates implements AutoCloseable {
        private final java.nio.file.Path file;
        private DataOutputStream output;
        private long count;

        private Candidates(java.nio.file.Path directory) throws IOException {
            Files.createDirectories(directory);
            file = Files.createTempFile(directory, "iceberg-orphans-", ".bin");
            try {
                output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)));
            } catch (IOException e) {
                Files.deleteIfExists(file);
                throw e;
            }
        }

        private void add(String path) {
            try {
                byte[] bytes = path.getBytes(StandardCharsets.UTF_8);
                output.writeInt(bytes.length);
                output.write(bytes);
                count++;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void finish() throws IOException {
            output.close();
            output = null;
        }

        private DataInputStream open() throws IOException {
            return new DataInputStream(new BufferedInputStream(Files.newInputStream(file)));
        }

        private static String read(DataInputStream input) throws IOException {
            byte[] bytes = new byte[input.readInt()];
            input.readFully(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public void close() throws IOException {
            try {
                if (output != null) {
                    output.close();
                }
            } finally {
                Files.deleteIfExists(file);
            }
        }
    }
}
