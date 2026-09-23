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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.Ref;
import org.lance.Version;
import org.lance.WriteParams;
import org.lance.cleanup.CleanupPolicy;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Opens a namespace-managed Lance table through a REST namespace stub.
 *
 * <p>The stub owns the version list, the way a namespace with managed versioning does:
 * DescribeTable reports {@code managed_versioning=true}, ListTableVersions and
 * DescribeTableVersion answer from a static list, and the dataset itself is a real local Lance
 * dataset with three versions written through the SDK. The test checks that the FE resolves
 * versions through the namespace, that a version the namespace does not record is unreachable,
 * and that the resulting metadata still carries the URI and storage options the BE needs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LanceManagedVersioningTest {
    private static final String BEARER_TOKEN = "managed-test-token";
    private static final String FULL_TABLE = "managed_full";
    private static final String PARTIAL_TABLE = "managed_partial";
    private static final String PLAIN_TABLE = "plain";
    private static final String DECLARED_TABLE = "declared_only";
    private static final String EXPIRED_TABLE = "plain_expired";
    /** Managed, records versions 1 and 3, but reports no commit times. */
    private static final String UNTIMED_TABLE = "managed_untimed";
    /** Managed, records only versions 1 and 2 while storage already holds version 3. */
    private static final String LAGGING_TABLE = "managed_lagging";
    /** Managed; its version 3 manifest is still at the staged path the namespace recorded. */
    private static final String STAGED_TABLE = "managed_staged";
    /** Managed; the namespace records more versions over time. */
    private static final String GROWING_TABLE = "managed_growing";
    /** Managed; DescribeTableVersion fails with a server error for version 2. */
    private static final String FLAKY_TABLE = "managed_flaky";
    /** Managed; DescribeTable returns a table_uri but no location. */
    private static final String NO_LOCATION_TABLE = "managed_no_location";
    /** Managed; DescribeTable returns a location that differs from table_uri. */
    private static final String MISMATCH_TABLE = "managed_mismatch";
    /** Managed; ListTableVersions without a page size returns one version per page with a token. */
    private static final String PAGED_TABLE = "managed_paged";
    /** Managed; the namespace lists no versions at all. */
    private static final String EMPTY_TABLE = "managed_empty";
    /** A branch of time_travel.lance forked from version 2 with one extra append (row 100). */
    private static final String BRANCH = "dev";
    /** A tag pointing at version 3 of the branch. */
    private static final String BRANCH_TAG = "rel";
    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private final List<String> requestPaths = new CopyOnWriteArrayList<>();
    private final List<String> requestQueries = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private String restUri;
    private String datasetUri;
    private String datasetStorePath;
    private List<Version> datasetVersions;
    private long branchVersions;
    private List<Version> branchDatasetVersions;
    private String expiredDatasetUri;
    private String stagedDatasetUri;
    private Path stagedCanonicalManifest;
    private String stagedManifestStorePath;
    /** Versions the stub namespace records per managed table; null means storage-versioned. */
    private final Map<String, List<Long>> namespaceVersions = new java.util.concurrent.ConcurrentHashMap<>();

    private Path tempDir;

    @BeforeAll
    public void setUp() throws Exception {
        LanceJniTestSupport.assumeJniBindingsLoadable();
        tempDir = Files.createTempDirectory("lance_managed_versioning");
        Path datasetDir = tempDir.resolve("time_travel.lance");
        datasetUri = datasetDir.toUri().toString();
        // Lance's object store addresses local files by their absolute path without the
        // leading slash, which is the form a namespace records in TableVersion.manifest_path.
        datasetStorePath = datasetDir.toAbsolutePath().toString().replaceFirst("^/", "");
        datasetVersions = writeThreeVersions(datasetUri);
        Assertions.assertEquals(3, datasetVersions.size());
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(datasetUri, allocator)) {
            dataset.tags().create("v2", 2);
            try (Dataset branch = dataset.createBranch(BRANCH, Ref.ofMain(2))) {
                Assertions.assertEquals(2, branch.version());
            }
            appendRows(datasetUri + "/tree/" + BRANCH, allocator, 2, 100, 100);
            // A tag that points into the branch, not at main's version 3.
            dataset.tags().create(BRANCH_TAG, 3, BRANCH);
        }
        branchVersions = 3;
        try (BufferAllocator allocator = new RootAllocator();
                Dataset branch = Dataset.open(datasetUri + "/tree/" + BRANCH, allocator)) {
            branchDatasetVersions = branch.listVersions();
        }
        // A second dataset whose two oldest versions were removed by cleanup, so that an explicit
        // selector can ask for a version that once existed but is no longer readable.
        expiredDatasetUri = tempDir.resolve("expired.lance").toUri().toString();
        writeThreeVersions(expiredDatasetUri);
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(expiredDatasetUri, allocator)) {
            dataset.cleanupWithPolicy(CleanupPolicy.builder().withBeforeVersion(3)
                    .withDeleteUnverified(true).build());
            Assertions.assertEquals(1, dataset.listVersions().size(), "cleanup must leave only version 3");
        }

        // A third dataset whose version 3 manifest sits at the staged path a namespace records
        // right after a commit, before anything finalizes it to the canonical path.
        Path stagedDir = tempDir.resolve("staged.lance");
        stagedDatasetUri = stagedDir.toUri().toString();
        writeThreeVersions(stagedDatasetUri);
        stagedCanonicalManifest = stagedDir.resolve("_versions")
                .resolve(U64_MAX.subtract(BigInteger.valueOf(3)) + ".manifest");
        Path stagedManifest = stagedDir.resolve("_versions")
                .resolve(stagedCanonicalManifest.getFileName() + "-2f6a1c0e-5b7d-4e1a-9c3b-0d8e7f6a5b4c");
        Files.move(stagedCanonicalManifest, stagedManifest);
        Files.deleteIfExists(stagedDir.resolve("_versions").resolve("latest_version_hint.json"));
        stagedManifestStorePath = stagedManifest.toAbsolutePath().toString().replaceFirst("^/", "");

        namespaceVersions.put(FULL_TABLE, Arrays.asList(1L, 2L, 3L));
        namespaceVersions.put(PARTIAL_TABLE, Arrays.asList(1L, 3L));
        namespaceVersions.put(UNTIMED_TABLE, Arrays.asList(1L, 3L));
        namespaceVersions.put(LAGGING_TABLE, Arrays.asList(1L, 2L));
        namespaceVersions.put(STAGED_TABLE, Arrays.asList(1L, 2L, 3L));
        namespaceVersions.put(GROWING_TABLE, Arrays.asList(1L, 2L));
        namespaceVersions.put(FLAKY_TABLE, Arrays.asList(1L, 2L, 3L));
        namespaceVersions.put(NO_LOCATION_TABLE, Arrays.asList(1L, 2L, 3L));
        namespaceVersions.put(MISMATCH_TABLE, Arrays.asList(1L, 2L, 3L));
        namespaceVersions.put(PAGED_TABLE, Arrays.asList(1L, 2L, 3L));
        namespaceVersions.put(EMPTY_TABLE, Collections.emptyList());

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handleRequest);
        server.start();
        restUri = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    }

    @AfterAll
    public void tearDown() throws IOException {
        if (server != null) {
            server.stop(0);
        }
        if (tempDir != null) {
            try (java.util.stream.Stream<Path> paths = Files.walk(tempDir)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    @Test
    public void testManagedTableResolvesVersionsThroughNamespace() throws Exception {
        LanceExternalCatalog catalog = newCatalog(300, "lance_managed_full");
        try {
            requestPaths.clear();
            LanceTableMetadata latest = catalog.loadTableMetadata("default", FULL_TABLE);
            Assertions.assertTrue(latest.isManagedVersioning());
            Assertions.assertEquals(3, latest.getVersion());
            Assertions.assertEquals(2, latest.getFragments().size());
            Assertions.assertEquals(datasetUri, latest.getDatasetUri());
            Assertions.assertTrue(requestPaths.stream().anyMatch(
                    path -> path.endsWith("/" + FULL_TABLE + "/version/list")),
                    "latest version must come from ListTableVersions: " + requestPaths);

            requestPaths.clear();
            LanceTableMetadata version2 = catalog.loadTableMetadata("default", FULL_TABLE,
                    Optional.of(new TableSnapshot("2", TableSnapshot.VersionType.VERSION)));
            Assertions.assertTrue(version2.isManagedVersioning());
            Assertions.assertEquals(2, version2.getVersion());
            Assertions.assertEquals(1, version2.getFragments().size());
            Assertions.assertTrue(requestPaths.stream().anyMatch(
                    path -> path.endsWith("/" + FULL_TABLE + "/version/describe")),
                    "explicit version must come from DescribeTableVersion: " + requestPaths);

            // FOR TIME AS OF is parsed in the Doris session time zone; format the instant just
            // after the second commit in that zone so the selector resolves to version 2.
            String betweenSecondAndThird = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(TimeUtils.getTimeZone().toZoneId())
                    .format(datasetVersions.get(1).getDataTime().toInstant().plusMillis(1));
            LanceTableMetadata byTime = catalog.loadTableMetadata("default", FULL_TABLE,
                    Optional.of(new TableSnapshot(betweenSecondAndThird, TableSnapshot.VersionType.TIME)));
            Assertions.assertEquals(2, byTime.getVersion());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testVersionMissingFromNamespaceIsReported() {
        LanceExternalCatalog catalog = newCatalog(301, "lance_managed_partial");
        try {
            LanceTableMetadata latest = catalog.loadTableMetadata("default", PARTIAL_TABLE);
            Assertions.assertEquals(3, latest.getVersion());
            LanceTableMetadata version1 = catalog.loadTableMetadata("default", PARTIAL_TABLE,
                    Optional.of(new TableSnapshot("1", TableSnapshot.VersionType.VERSION)));
            Assertions.assertEquals(1, version1.getVersion());
            Assertions.assertTrue(version1.getFragments().isEmpty());

            // Version 2 exists in storage, but the namespace does not record it.
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PARTIAL_TABLE,
                            Optional.of(new TableSnapshot("2", TableSnapshot.VersionType.VERSION))));
            Assertions.assertEquals(
                    "Lance version 2 of default." + PARTIAL_TABLE + " was not found in the namespace",
                    exception.getMessage());

            // FOR TIME AS OF uses the commit times the namespace reports, so an instant after the
            // second commit selects version 1, the latest version the namespace records by then.
            requestPaths.clear();
            String betweenSecondAndThird = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(TimeUtils.getTimeZone().toZoneId())
                    .format(datasetVersions.get(1).getDataTime().toInstant().plusMillis(1));
            LanceTableMetadata byTime = catalog.loadTableMetadata("default", PARTIAL_TABLE,
                    Optional.of(new TableSnapshot(betweenSecondAndThird, TableSnapshot.VersionType.TIME)));
            Assertions.assertEquals(1, byTime.getVersion());
            Assertions.assertTrue(requestPaths.stream().anyMatch(
                    path -> path.endsWith("/" + PARTIAL_TABLE + "/version/list")),
                    "time travel must read commit times from ListTableVersions: " + requestPaths);

            // A namespace that lists versions without commit times: the times come from the
            // manifests in storage, but only the listed versions are candidates, so the same
            // instant still selects version 1 rather than the storage-only version 2.
            LanceTableMetadata untimed = catalog.loadTableMetadata("default", UNTIMED_TABLE,
                    Optional.of(new TableSnapshot(betweenSecondAndThird, TableSnapshot.VersionType.TIME)));
            Assertions.assertEquals(1, untimed.getVersion());
            Assertions.assertTrue(untimed.isManagedVersioning());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testStorageVersionedTableStillOpensByUri() {
        LanceExternalCatalog catalog = newCatalog(302, "lance_managed_plain");
        try {
            requestPaths.clear();
            LanceTableMetadata latest = catalog.loadTableMetadata("default", PLAIN_TABLE);
            Assertions.assertFalse(latest.isManagedVersioning());
            Assertions.assertEquals(3, latest.getVersion());
            Assertions.assertTrue(requestPaths.stream().noneMatch(path -> path.contains("/version/")),
                    "a storage-versioned table must not consult namespace versions: " + requestPaths);

            // Version 2 is not recorded by the namespace either, but this table does not ask it.
            LanceTableMetadata version2 = catalog.loadTableMetadata("default", PLAIN_TABLE,
                    Optional.of(new TableSnapshot("2", TableSnapshot.VersionType.VERSION)));
            Assertions.assertEquals(2, version2.getVersion());

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PLAIN_TABLE,
                            Optional.of(new TableSnapshot("99", TableSnapshot.VersionType.VERSION))));
            Assertions.assertEquals("Lance version 99 of default." + PLAIN_TABLE + " was not found",
                    exception.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testLatestVersionComesFromNamespaceNotStorage() {
        LanceExternalCatalog catalog = newCatalog(305, "lance_managed_lagging");
        try {
            // Storage holds version 3, but the namespace has recorded only up to version 2, so
            // the table's latest version, by number and by time, is 2.
            LanceTableMetadata latest = catalog.loadTableMetadata("default", LAGGING_TABLE);
            Assertions.assertEquals(2, latest.getVersion());
            Assertions.assertEquals(1, latest.getFragments().size());
            String afterThirdCommit = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(TimeUtils.getTimeZone().toZoneId())
                    .format(datasetVersions.get(2).getDataTime().toInstant().plusMillis(1));
            LanceTableMetadata byTime = catalog.loadTableMetadata("default", LAGGING_TABLE,
                    Optional.of(new TableSnapshot(afterThirdCommit, TableSnapshot.VersionType.TIME)));
            Assertions.assertEquals(2, byTime.getVersion());
            RuntimeException ahead = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", LAGGING_TABLE,
                            Optional.of(new TableSnapshot("3", TableSnapshot.VersionType.VERSION))));
            Assertions.assertEquals("Lance version 3 of default." + LAGGING_TABLE + " was not found in the namespace",
                    ahead.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testStagedManifestIsFinalizedForTheBeReader() throws Exception {
        Assertions.assertFalse(Files.exists(stagedCanonicalManifest), "fixture must start with a staged manifest");
        LanceExternalCatalog catalog = newCatalog(306, "lance_managed_staged");
        try {
            LanceTableMetadata latest = catalog.loadTableMetadata("default", STAGED_TABLE);
            Assertions.assertEquals(3, latest.getVersion());
            Assertions.assertEquals(2, latest.getFragments().size());
        } finally {
            catalog.onClose();
        }
        // The SDK copied the staged manifest to its canonical path while opening through the
        // namespace, so a reader that only knows the URI and the version number, which is how
        // the BE opens the dataset, now sees version 3 as well.
        Assertions.assertTrue(Files.exists(stagedCanonicalManifest), "manifest must be finalized on read");
        try (BufferAllocator allocator = new RootAllocator();
                Dataset byUri = Dataset.open().allocator(allocator).uri(stagedDatasetUri)
                        .readOptions(new org.lance.ReadOptions.Builder().setVersion(3).build()).build()) {
            Assertions.assertEquals(3, byUri.version());
            Assertions.assertEquals(2, byUri.getFragments().size());
        }
    }

    @Test
    public void testNamespaceVersionsAreNotPinnedByTheAccessCache() {
        LanceExternalCatalog catalog = newCatalog(307, "lance_managed_growing");
        try {
            // No vended credentials, so the resolved table access is cached; the version must
            // still be resolved through the namespace on every read.
            Assertions.assertEquals(2, catalog.loadTableMetadata("default", GROWING_TABLE).getVersion());
            namespaceVersions.put(GROWING_TABLE, Arrays.asList(1L, 2L, 3L));
            Assertions.assertEquals(3, catalog.loadTableMetadata("default", GROWING_TABLE).getVersion());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testNamespaceFailureIsNotReportedAsMissingVersion() {
        LanceExternalCatalog catalog = newCatalog(308, "lance_managed_flaky");
        try {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", FLAKY_TABLE,
                            Optional.of(new TableSnapshot("2", TableSnapshot.VersionType.VERSION))));
            Assertions.assertTrue(failure.getMessage().startsWith(
                    "Failed to load Lance table metadata for default." + FLAKY_TABLE + ": "), failure.getMessage());
            Assertions.assertFalse(failure.getMessage().contains("was not found"), failure.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testManagedTableRequiresConsistentLocation() {
        LanceExternalCatalog catalog = newCatalog(309, "lance_managed_location");
        try {
            RuntimeException noLocation = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", NO_LOCATION_TABLE));
            Assertions.assertTrue(noLocation.getMessage().contains(
                    "returned no location for managed table"), noLocation.getMessage());
            RuntimeException mismatch = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", MISMATCH_TABLE));
            Assertions.assertTrue(mismatch.getMessage().contains(
                    "table_uri that differs from location"), mismatch.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testTagSelectorsResolveToVersions() {
        LanceExternalCatalog catalog = newCatalog(310, "lance_managed_tags");
        try {
            // Storage-versioned: the tag lives in the dataset's _refs/tags/.
            Assertions.assertEquals(2, catalog.loadTableMetadata("default", PLAIN_TABLE,
                    LanceRefSelector.tag("v2")).getVersion());
            RuntimeException plainMissing = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PLAIN_TABLE, LanceRefSelector.tag("nope")));
            Assertions.assertEquals("Lance tag 'nope' of default." + PLAIN_TABLE + " was not found",
                    plainMissing.getMessage());

            // Managed: tags live in the same _refs/tags/, and the selected version is then
            // resolved through the namespace like an explicit one.
            requestPaths.clear();
            LanceTableMetadata managed = catalog.loadTableMetadata("default", FULL_TABLE, LanceRefSelector.tag("v2"));
            Assertions.assertEquals(2, managed.getVersion());
            Assertions.assertTrue(managed.isManagedVersioning());
            Assertions.assertTrue(requestPaths.stream().anyMatch(
                    path -> path.endsWith("/" + FULL_TABLE + "/version/describe")), requestPaths.toString());
            RuntimeException managedMissing = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", FULL_TABLE, LanceRefSelector.tag("nope")));
            Assertions.assertEquals("Lance tag 'nope' of default." + FULL_TABLE + " was not found",
                    managedMissing.getMessage());
            // A tag that points at a version the namespace no longer records.
            RuntimeException dropped = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PARTIAL_TABLE, LanceRefSelector.tag("v2")));
            Assertions.assertEquals("Lance version 2 of default." + PARTIAL_TABLE
                    + " (tag 'v2') was not found in the namespace", dropped.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testBranchSelectorsOpenTheBranchChain() {
        LanceExternalCatalog catalog = newCatalog(311, "lance_managed_branches");
        try {
            // Storage-versioned: the branch is another manifest chain under <table>/tree/<branch>,
            // which is what the BE receives as the dataset URI.
            LanceTableMetadata dev = catalog.loadTableMetadata("default", PLAIN_TABLE,
                    LanceRefSelector.branch(BRANCH, Optional.empty()));
            Assertions.assertEquals(3, dev.getVersion());
            Assertions.assertEquals(2, dev.getFragments().size());
            Assertions.assertEquals(Optional.of(BRANCH), dev.getBranch());
            Assertions.assertTrue(dev.getDatasetUri().endsWith("/tree/" + BRANCH), dev.getDatasetUri());
            LanceTableMetadata devV2 = catalog.loadTableMetadata("default", PLAIN_TABLE, LanceRefSelector.branch(
                    BRANCH, Optional.of(new TableSnapshot("2", TableSnapshot.VersionType.VERSION))));
            Assertions.assertEquals(2, devV2.getVersion());
            Assertions.assertEquals(1, devV2.getFragments().size());
            RuntimeException missing = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PLAIN_TABLE,
                            LanceRefSelector.branch("nope", Optional.empty())));
            Assertions.assertEquals("Lance branch 'nope' of default." + PLAIN_TABLE + " was not found",
                    missing.getMessage());
            RuntimeException missingVersion = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PLAIN_TABLE, LanceRefSelector.branch(
                            BRANCH, Optional.of(new TableSnapshot("9", TableSnapshot.VersionType.VERSION)))));
            Assertions.assertEquals("Lance version 9 of default." + PLAIN_TABLE + "@" + BRANCH + " was not found",
                    missingVersion.getMessage());

            // Managed: the namespace records the branch's versions and the SDK asks it with the
            // branch name derived from the branch directory.
            requestQueries.clear();
            LanceTableMetadata managedDev = catalog.loadTableMetadata("default", FULL_TABLE,
                    LanceRefSelector.branch(BRANCH, Optional.empty()));
            Assertions.assertEquals(3, managedDev.getVersion());
            Assertions.assertTrue(managedDev.isManagedVersioning());
            Assertions.assertTrue(requestQueries.stream().anyMatch(q -> q.contains("branch=" + BRANCH)),
                    "the namespace must be asked for the branch: " + requestQueries);
            RuntimeException managedMissing = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", FULL_TABLE,
                            LanceRefSelector.branch("nope", Optional.empty())));
            Assertions.assertEquals("Lance branch 'nope' of default." + FULL_TABLE + " was not found",
                    managedMissing.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testTagOnBranchSelectsTheBranch() {
        LanceExternalCatalog catalog = newCatalog(312, "lance_managed_branch_tags");
        try {
            // Storage-versioned: the tag's branch comes from _refs/tags/<tag>.json.
            LanceTableMetadata plain = catalog.loadTableMetadata("default", PLAIN_TABLE,
                    LanceRefSelector.tag(BRANCH_TAG));
            Assertions.assertEquals(3, plain.getVersion());
            Assertions.assertEquals(Optional.of(BRANCH), plain.getBranch());
            Assertions.assertTrue(plain.getDatasetUri().endsWith("/tree/" + BRANCH), plain.getDatasetUri());
            Assertions.assertEquals(2, plain.getFragments().size(), "branch version 3, not main version 3");
            // Managed: the same tag, with the branch checked out through the namespace.
            LanceTableMetadata managed = catalog.loadTableMetadata("default", FULL_TABLE,
                    LanceRefSelector.tag(BRANCH_TAG));
            Assertions.assertEquals(3, managed.getVersion());
            Assertions.assertEquals(Optional.of(BRANCH), managed.getBranch());
            Assertions.assertEquals(2, managed.getFragments().size());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testTimeTravelInsideBranchUsesBranchCommitTimes() {
        LanceExternalCatalog catalog = newCatalog(313, "lance_managed_branch_time");
        try {
            // Between the branch's version 2 (the fork point) and its own version 3.
            String betweenBranchCommits = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(TimeUtils.getTimeZone().toZoneId())
                    .format(branchDatasetVersions.stream().filter(v -> v.getId() == 3).findFirst()
                            .orElseThrow(() -> new AssertionError("branch must have version 3"))
                            .getDataTime().toInstant().minusMillis(1));
            TableSnapshot time = new TableSnapshot(betweenBranchCommits, TableSnapshot.VersionType.TIME);
            LanceTableMetadata plain = catalog.loadTableMetadata("default", PLAIN_TABLE,
                    LanceRefSelector.branch(BRANCH, Optional.of(time)));
            Assertions.assertEquals(2, plain.getVersion());
            Assertions.assertEquals(Optional.of(BRANCH), plain.getBranch());
            // Managed: the stub lists the branch's versions without commit times, so the times
            // come from the branch manifests, restricted to the versions the namespace lists.
            requestQueries.clear();
            LanceTableMetadata managed = catalog.loadTableMetadata("default", FULL_TABLE,
                    LanceRefSelector.branch(BRANCH, Optional.of(time)));
            Assertions.assertEquals(2, managed.getVersion());
            Assertions.assertTrue(requestQueries.stream().anyMatch(q -> q.contains("branch=" + BRANCH)),
                    requestQueries.toString());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testNamespaceVersionsArePagedToTheEnd() {
        LanceExternalCatalog catalog = newCatalog(314, "lance_managed_paged");
        try {
            // Version 3 is only on the last page; any early stop or page size would miss it.
            String afterThirdCommit = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(TimeUtils.getTimeZone().toZoneId())
                    .format(datasetVersions.get(2).getDataTime().toInstant().plusMillis(1));
            requestQueries.clear();
            requestPaths.clear();
            LanceTableMetadata byTime = catalog.loadTableMetadata("default", PAGED_TABLE,
                    Optional.of(new TableSnapshot(afterThirdCommit, TableSnapshot.VersionType.TIME)));
            Assertions.assertEquals(3, byTime.getVersion());
            long pages = requestQueries.stream().filter(q -> q.contains("page_token=")).count();
            Assertions.assertEquals(2, pages, "every page after the first must be requested: " + requestQueries);
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testEmptyNamespaceVersionListIsReported() {
        LanceExternalCatalog catalog = newCatalog(315, "lance_managed_empty");
        try {
            RuntimeException empty = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", EMPTY_TABLE,
                            Optional.of(new TableSnapshot("2030-01-01 00:00:00", TableSnapshot.VersionType.TIME))));
            Assertions.assertEquals("Lance namespace lists no versions for default." + EMPTY_TABLE,
                    empty.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testCleanedUpVersionIsReportedAsNotFound() {
        LanceExternalCatalog catalog = newCatalog(304, "lance_managed_expired");
        try {
            LanceTableMetadata latest = catalog.loadTableMetadata("default", EXPIRED_TABLE);
            Assertions.assertEquals(3, latest.getVersion());
            Assertions.assertEquals(3, catalog.loadTableMetadata("default", EXPIRED_TABLE,
                    Optional.of(new TableSnapshot("3", TableSnapshot.VersionType.VERSION))).getVersion());

            // Version 1 was committed and later removed by cleanup: same error as a version that
            // never existed, without the storage path of the missing manifest.
            RuntimeException expired = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", EXPIRED_TABLE,
                            Optional.of(new TableSnapshot("1", TableSnapshot.VersionType.VERSION))));
            Assertions.assertEquals("Lance version 1 of default." + EXPIRED_TABLE + " was not found",
                    expired.getMessage());
            Assertions.assertFalse(String.valueOf(expired.getCause().getMessage()).contains(tempDir.toString()),
                    String.valueOf(expired.getCause().getMessage()));

            // FOR TIME AS OF only sees the versions that still exist, so a time before the
            // surviving version has nothing to select.
            RuntimeException tooEarly = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", EXPIRED_TABLE,
                            Optional.of(new TableSnapshot("2000-01-01 00:00:00", TableSnapshot.VersionType.TIME))));
            Assertions.assertTrue(tooEarly.getMessage().contains(
                    "Lance dataset has no version at or before '2000-01-01 00:00:00'"), tooEarly.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testSelectorErrorsKeepTheirMessages() {
        LanceExternalCatalog catalog = newCatalog(303, "lance_managed_errors");
        try {
            RuntimeException nonNumeric = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PLAIN_TABLE,
                            Optional.of(new TableSnapshot("abc", TableSnapshot.VersionType.VERSION))));
            Assertions.assertTrue(nonNumeric.getMessage().contains(
                    "Lance FOR VERSION AS OF requires a numeric version, but was 'abc'"), nonNumeric.getMessage());

            RuntimeException tooEarly = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PLAIN_TABLE,
                            Optional.of(new TableSnapshot("2000-01-01 00:00:00", TableSnapshot.VersionType.TIME))));
            Assertions.assertTrue(tooEarly.getMessage().contains(
                    "Lance dataset has no version at or before '2000-01-01 00:00:00'"), tooEarly.getMessage());

            RuntimeException unparsable = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", PLAIN_TABLE,
                            Optional.of(new TableSnapshot("not-a-time", TableSnapshot.VersionType.TIME))));
            Assertions.assertTrue(unparsable.getMessage().contains(
                    "Cannot parse Lance FOR TIME AS OF value 'not-a-time'"), unparsable.getMessage());

            RuntimeException declared = Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.loadTableMetadata("default", DECLARED_TABLE));
            Assertions.assertTrue(declared.getMessage().startsWith(
                    "Failed to load Lance table metadata for default." + DECLARED_TABLE + ": "), declared.getMessage());
            Assertions.assertTrue(declared.getMessage().endsWith(
                    "Lance table is declared in the namespace but has no data yet"), declared.getMessage());
        } finally {
            catalog.onClose();
        }
    }

    private LanceExternalCatalog newCatalog(long id, String name) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE, LanceExternalCatalog.LANCE_REST);
        properties.put(LanceExternalCatalog.REST_URI, restUri);
        properties.put(LanceExternalCatalog.REST_SECURITY_TYPE, "bearer");
        properties.put(LanceExternalCatalog.REST_BEARER_TOKEN, BEARER_TOKEN);
        return new LanceExternalCatalog(id, name, null, properties, "");
    }

    /** Writes a dataset with three versions: an empty create, then two one-fragment appends. */
    private static List<Version> writeThreeVersions(String uri) throws Exception {
        Schema schema = new Schema(Collections.singletonList(
                new Field("row_id", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
        try (BufferAllocator allocator = new RootAllocator()) {
            WriteParams params = new WriteParams.Builder().withDataStorageVersion("2.0").build();
            try (Dataset created = Dataset.create(allocator, uri, schema, params)) {
                Assertions.assertEquals(1, created.version());
            }
            long readVersion = 1;
            for (int append = 0; append < 2; append++) {
                readVersion = appendRows(uri, allocator, readVersion, append * 3 + 1, append * 3 + 3);
                Thread.sleep(20);
            }
            try (Dataset dataset = Dataset.open(uri, allocator)) {
                return dataset.listVersions();
            }
        }
    }

    /** Appends one fragment holding row_id {@code first..last} and returns the committed version. */
    private static long appendRows(String uri, BufferAllocator allocator, long readVersion, int first, int last)
            throws Exception {
        Schema schema = new Schema(Collections.singletonList(
                new Field("row_id", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
        WriteParams params = new WriteParams.Builder().withDataStorageVersion("2.0").build();
        int count = last - first + 1;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector rowId = (IntVector) root.getVector("row_id");
            rowId.allocateNew(count);
            for (int i = 0; i < count; i++) {
                rowId.set(i, first + i);
            }
            rowId.setValueCount(count);
            root.setRowCount(count);
            List<FragmentMetadata> fragments = Fragment.create(uri, allocator, root, params);
            try (Dataset committed = new FragmentOperation.Append(fragments)
                    .commit(allocator, uri, Optional.of(readVersion), Collections.emptyMap())) {
                return committed.version();
            }
        }
    }

    // Three versions are written with V2 manifest naming, so version v is u64::MAX - v.
    private String manifestPath(String table, long version, String branch) {
        if (STAGED_TABLE.equals(table) && version == 3) {
            return stagedManifestStorePath;
        }
        String root = STAGED_TABLE.equals(table)
                ? Paths.get(java.net.URI.create(stagedDatasetUri)).toAbsolutePath().toString().replaceFirst("^/", "")
                : datasetStorePath;
        if (branch != null) {
            root = root + "/tree/" + branch;
        }
        return root + "/_versions/" + U64_MAX.subtract(BigInteger.valueOf(version)) + ".manifest";
    }

    /** The versions the stub namespace records on a branch; only FULL_TABLE has the "dev" branch. */
    private List<Long> namespaceBranchVersions(String table, String branch) {
        if (FULL_TABLE.equals(table) && BRANCH.equals(branch)) {
            List<Long> versions = new java.util.ArrayList<>();
            for (long v = 2; v <= branchVersions; v++) {
                versions.add(v);
            }
            return versions;
        }
        return null;
    }

    /** The location DescribeTable reports; the expired table has its own dataset. */
    private String locationOf(String table) {
        if (EXPIRED_TABLE.equals(table)) {
            return expiredDatasetUri;
        }
        if (STAGED_TABLE.equals(table)) {
            return stagedDatasetUri;
        }
        if (NO_LOCATION_TABLE.equals(table)) {
            return "";
        }
        return datasetUri;
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        byte[] body = ByteStreams.toByteArray(exchange.getRequestBody());
        String path = exchange.getRequestURI().getPath();
        String query = String.valueOf(exchange.getRequestURI().getQuery());
        requestPaths.add(path);
        requestQueries.add(query);

        int status = 200;
        String response;
        if (!("Bearer " + BEARER_TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
            status = 401;
            response = "{\"error\":\"unauthorized\",\"code\":16}";
        } else if (path.matches("/v1/table/[^/]+/version/list")) {
            String table = path.split("/")[3];
            java.util.regex.Matcher branchMatcher = java.util.regex.Pattern.compile("(?:^|&)branch=([^&]+)")
                    .matcher(query);
            String branch = branchMatcher.find()
                    ? java.net.URLDecoder.decode(branchMatcher.group(1), "UTF-8") : null;
            List<Long> versions = branch == null ? namespaceVersions.get(table)
                    : namespaceBranchVersions(table, branch);
            if (branch != null && versions == null) {
                status = 404;
                response = "{\"error\":\"table branch " + branch + " not found\",\"code\":22}";
            } else if (versions == null) {
                status = 400;
                response = "{\"error\":\"versions are not managed\",\"code\":13}";
            } else if (PAGED_TABLE.equals(table) && !query.contains("limit=")) {
                // One version per page, oldest first, with the index of the next one as the token.
                java.util.regex.Matcher tokenMatcher = java.util.regex.Pattern.compile("(?:^|&)page_token=(\\d+)")
                        .matcher(query);
                int start = tokenMatcher.find() ? Integer.parseInt(tokenMatcher.group(1)) : 0;
                response = "{\"versions\":[" + tableVersionJson(table, versions.get(start), branch) + "]"
                        + (start + 1 < versions.size() ? ",\"page_token\":\"" + (start + 1) + "\"" : "") + "}";
            } else {
                boolean descending = query.contains("descending=true");
                java.util.regex.Matcher limitMatcher = java.util.regex.Pattern.compile("(?:^|&)limit=(\\d+)")
                        .matcher(query);
                int limit = limitMatcher.find() ? Integer.parseInt(limitMatcher.group(1)) : versions.size();
                StringBuilder entries = new StringBuilder();
                for (int i = 0; i < Math.min(limit, versions.size()); i++) {
                    long version = versions.get(descending ? versions.size() - 1 - i : i);
                    if (entries.length() > 0) {
                        entries.append(',');
                    }
                    entries.append(tableVersionJson(table, version, branch));
                }
                response = "{\"versions\":[" + entries + "]}";
            }
        } else if (path.matches("/v1/table/[^/]+/version/describe")) {
            String table = path.split("/")[3];
            JsonNode request = JsonUtil.readTree(new String(body, StandardCharsets.UTF_8));
            JsonNode requested = request.get("version");
            String branch = request.hasNonNull("branch") ? request.get("branch").asText() : null;
            List<Long> versions = branch == null ? namespaceVersions.get(table)
                    : namespaceBranchVersions(table, branch);
            long version = requested == null ? -1 : requested.asLong();
            if (branch != null && versions == null) {
                status = 404;
                response = "{\"error\":\"table branch " + branch + " not found\",\"code\":22}";
            } else if (FLAKY_TABLE.equals(table) && version == 2) {
                status = 500;
                response = "{\"error\":\"version store unavailable\",\"code\":18}";
            } else if (versions == null || !versions.contains(version)) {
                status = 404;
                response = "{\"error\":\"table version " + version + " not found\",\"code\":11}";
            } else {
                response = "{\"version\":" + tableVersionJson(table, version, branch) + "}";
            }
        } else if (path.matches("/v1/table/[^/]+/describe")) {
            String table = path.split("/")[3];
            boolean managed = namespaceVersions.containsKey(table);
            boolean declared = DECLARED_TABLE.equals(table);
            boolean plain = PLAIN_TABLE.equals(table) || EXPIRED_TABLE.equals(table);
            if (!managed && !plain && !declared) {
                status = 404;
                response = "{\"error\":\"table not found\",\"code\":4}";
            } else {
                String location = locationOf(table);
                String tableUri = MISMATCH_TABLE.equals(table) || NO_LOCATION_TABLE.equals(table)
                        ? expiredDatasetUri : location;
                response = "{\"table\":\"" + table + "\",\"namespace\":[],"
                        + "\"location\":\"" + location + "\",\"table_uri\":\"" + tableUri + "\","
                        + "\"storage_options\":{},"
                        + "\"managed_versioning\":" + managed + ","
                        + "\"is_only_declared\":" + declared + "}";
            }
        } else if (path.endsWith("/table/list")) {
            response = "{\"tables\":[]}";
        } else if (path.endsWith("/list")) {
            response = "{\"namespaces\":[]}";
        } else {
            status = 404;
            response = "{\"error\":\"not found\",\"code\":4}";
        }
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);
        exchange.close();
    }

    private String tableVersionJson(String table, long version, String branch) {
        String manifestPath = manifestPath(table, version, branch);
        long size;
        try {
            size = Files.size(Paths.get("/" + manifestPath));
        } catch (IOException e) {
            size = 0;
        }
        // Branch versions are reported without commit times, exercising the storage fallback.
        String commitTime = UNTIMED_TABLE.equals(table) || branch != null ? "" : ",\"timestamp_millis\":"
                + datasetVersions.get((int) version - 1).getDataTime().toInstant().toEpochMilli();
        return "{\"version\":" + version + ",\"manifest_path\":\"" + manifestPath
                + "\",\"manifest_size\":" + size + commitTime + "}";
    }
}
