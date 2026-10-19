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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Pins the {@code paimon.bucket} scan-range property (P4-2-a0).
 *
 * <p>WHY this property exists: a sibling connector can plan paimon splits on behalf of its OWN table
 * and then has to line each split up with its own per-bucket state. Today that sibling is the fluss
 * connector: a fluss table tiered into paimon keeps bucket-identical layout, and the fluss connector
 * binds the un-tiered log tail of bucket <i>b</i> to the lake splits of bucket <i>b</i>. Without the
 * bucket on the range there is nothing in a {@link PaimonScanRange} that says which bucket it came
 * from (the JNI arm carries only an opaque serialized split; the native arm only a file path), so the
 * sibling would have to fall back to whole-table binding — correct but wasteful — or parse the
 * sibling's internal directory layout, which the JNI arm does not even expose.
 *
 * <p>The fixture is deliberately MULTI-bucket: with a single bucket every range would carry "0" and a
 * hard-coded constant would pass. Each test therefore asserts the ranges reproduce the split-side
 * bucket SET, which a constant cannot.
 */
public class PaimonScanRangeBucketTest {

    /**
     * A two-bucket PK table with rows in BOTH buckets. PK {@code id} hashes into
     * {@code bucket = hash(id) % 2}; ids 1..8 cover both buckets for paimon's hash function.
     */
    private static Table createTwoBucketTable(Catalog catalog) throws Exception {
        catalog.createDatabase("db", false);
        Identifier id = Identifier.create("db", "t");
        catalog.createTable(id, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("val", DataTypes.BIGINT())
                .primaryKey("id")
                .option("bucket", "2")
                .build(), false);
        Table table = catalog.getTable(id);

        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite()) {
            for (int i = 1; i <= 8; i++) {
                write.write(GenericRow.of(i, (long) i * 100));
            }
            List<CommitMessage> messages = write.prepareCommit();
            try (BatchTableCommit commit = wb.newCommit()) {
                commit.commit(messages);
            }
        }
        return table;
    }

    /** The buckets paimon's own read plan reports — the reference the ranges must reproduce. */
    private static Set<Integer> planBuckets(Table table) throws Exception {
        Set<Integer> buckets = new TreeSet<>();
        for (Split s : table.newReadBuilder().newScan().plan().splits()) {
            if (s instanceof DataSplit) {
                buckets.add(((DataSplit) s).bucket());
            }
        }
        return buckets;
    }

    /** The buckets the planned ranges claim, as ints. Fails the test if any range omits the property. */
    private static Set<Integer> rangeBuckets(List<ConnectorScanRange> ranges) {
        Set<Integer> buckets = new TreeSet<>();
        for (ConnectorScanRange r : ranges) {
            String bucket = r.getProperties().get("paimon.bucket");
            Assertions.assertNotNull(bucket,
                    "every DataSplit-backed range must carry paimon.bucket; missing on " + r);
            buckets.add(Integer.parseInt(bucket));
        }
        return buckets;
    }

    private static PaimonScanPlanProvider providerFor(Table table) {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        return new PaimonScanPlanProvider(PaimonCatalogProperties.of(Collections.emptyMap()), ops);
    }

    private static PaimonTableHandle handleFor(String tableName) {
        return new PaimonTableHandle("db", tableName,
                Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void nativeRangesCarryTheBucketOfTheSplitTheyCameFrom(@TempDir Path warehouse)
            throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Table table = createTwoBucketTable(catalog);
            Set<Integer> expected = planBuckets(table);
            Assertions.assertTrue(expected.size() >= 2,
                    "fixture precondition: the table must really span >=2 buckets, got " + expected);

            List<ConnectorScanRange> ranges = providerFor(table).planScan(
                    sessionWithProps(Collections.emptyMap()),
                    ConnectorScanRequest.builder(handleFor("t"), noColumns()).build());

            Assertions.assertFalse(ranges.isEmpty(), "the fixture must plan at least one range");
            for (ConnectorScanRange r : ranges) {
                Assertions.assertTrue(((PaimonScanRange) r).isNativeReadRange(),
                        "fixture precondition: this arm must exercise the NATIVE range builder");
            }
            // WHY: a native range is one sub-range of one raw file of one DataSplit, so the bucket has
            // to be threaded down from the DataSplit loop through buildNativeRanges/buildNativeRange.
            // MUTATION: hard-coding 0 (or dropping .bucket() from the native builder) -> {0} instead of
            // {0, 1} -> red; the multi-bucket fixture is what makes the constant detectable.
            Assertions.assertEquals(expected, rangeBuckets(ranges),
                    "native ranges must reproduce exactly the buckets paimon's own plan reports");
        }
    }

    @Test
    public void jniRangesCarryTheBucketOfTheSplitTheyCameFrom(@TempDir Path warehouse)
            throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Table table = createTwoBucketTable(catalog);
            Set<Integer> expected = planBuckets(table);
            Assertions.assertTrue(expected.size() >= 2,
                    "fixture precondition: the table must really span >=2 buckets, got " + expected);

            List<ConnectorScanRange> ranges = providerFor(table).planScan(
                    sessionWithProps(Collections.singletonMap("force_jni_scanner", "true")),
                    ConnectorScanRequest.builder(handleFor("t"), noColumns()).build());

            Assertions.assertFalse(ranges.isEmpty(), "the fixture must plan at least one range");
            for (ConnectorScanRange r : ranges) {
                Assertions.assertTrue(r.getProperties().containsKey("paimon.split"),
                        "fixture precondition: this arm must exercise the JNI range builder");
            }
            // WHY: which BE reader a split ends up on (native vs JNI, a session-level escape hatch the
            // sibling does not control) must not change what the sibling can learn about the split.
            // If only the native arm carried the bucket, turning on force_jni_scanner would silently
            // break the sibling's binding. MUTATION: setting .bucket() only on the native arm -> the
            // rangeBuckets assertNotNull fires -> red.
            Assertions.assertEquals(expected, rangeBuckets(ranges),
                    "JNI ranges must reproduce exactly the buckets paimon's own plan reports");
        }
    }

    @Test
    public void collapsedCountRangeCarriesNoBucket(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Table table = createTwoBucketTable(catalog);
            Assertions.assertTrue(planBuckets(table).size() >= 2,
                    "fixture precondition: >=2 buckets, so the collapse really does span buckets");

            List<ConnectorScanRange> ranges = providerFor(table).planScan(
                    sessionWithProps(Collections.emptyMap()),
                    ConnectorScanRequest.builder(handleFor("t"), noColumns())
                            .countPushdown(true).build());

            // WHY: the count collapse folds the splits of ALL buckets into ONE range carrying the summed
            // total, so no single bucket number is true of it. Stamping the representative split's bucket
            // would hand a sibling a range that claims to be bucket b while actually standing for every
            // bucket — it would suppress/join against the wrong state. Absent is the honest answer, and
            // the sibling is required to fail loud rather than guess (it never forwards count pushdown,
            // so it must never see one of these).
            // MUTATION: adding .bucket() to buildCountRange -> the count range carries one -> red.
            int countRanges = 0;
            for (ConnectorScanRange r : ranges) {
                if (r.getProperties().containsKey("paimon.row_count")) {
                    ++countRanges;
                    Assertions.assertFalse(r.getProperties().containsKey("paimon.bucket"),
                            "the collapsed count range spans every bucket, so it must claim none");
                }
            }
            Assertions.assertEquals(1, countRanges,
                    "fixture precondition: count pushdown must produce exactly one collapsed range");
        }
    }

    @Test
    public void systemTableSplitCarriesNoBucket(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            createTwoBucketTable(catalog);
            Table snapshots = catalog.getTable(Identifier.create("db", "t$snapshots"));

            List<ConnectorScanRange> ranges = providerFor(snapshots).planScan(
                    sessionWithProps(Collections.emptyMap()),
                    ConnectorScanRequest.builder(handleFor("t$snapshots"), noColumns()).build());

            Assertions.assertFalse(ranges.isEmpty(), "a snapshots system table must plan >=1 range");
            // WHY: a system-table split is not a DataSplit and has no bucket at all — fabricating one
            // (say 0) would be a lie a sibling could act on. MUTATION: setting .bucket() unconditionally
            // in buildJniScanRange (dropping the isDataSplit gate) -> red. This also documents the shape
            // the sibling must reject: it plans only data reads, so a bucket-less range reaching its
            // wrapper means the contract broke.
            for (ConnectorScanRange r : ranges) {
                Assertions.assertFalse(r.getProperties().containsKey("paimon.bucket"),
                        "a non-DataSplit system split has no bucket, so it must not claim one");
            }
        }
    }

    private static List<ConnectorColumnHandle> noColumns() {
        return Collections.emptyList();
    }

    private static ConnectorSession sessionWithProps(Map<String, String> sessionProps) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "q";
            }

            @Override
            public String getUser() {
                return "u";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return sessionProps;
            }
        };
    }
}
