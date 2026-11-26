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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Comparators;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pins {@code rewrite_manifests}, including the delegated connector {@link RewriteManifestExecutor}.
 *
 * <p><b>WHY this matters:</b> the body short-circuits an empty table to {@code ["0", "0"]} (no executor
 * call), and otherwise reports (rewritten, added) manifest counts from the executor. The executor is the
 * fe-core port stripped of its {@code ExternalTable}/{@code ExtMetaCacheMgr} couplings; this verifies it
 * actually combines the per-append manifests through the SDK {@code rewriteManifests()} API.</p>
 */
public class IcebergRewriteManifestsActionTest {

    private static IcebergRewriteManifestsAction action(java.util.Map<String, String> props) {
        return new IcebergRewriteManifestsAction(props, Collections.emptyList(), null);
    }

    @Test
    public void emptyTableShortCircuitsToZeroZero() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");

        IcebergRewriteManifestsAction action = action(Collections.emptyMap());
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(ImmutableList.of("0", "0"), result.getRows().get(0),
                "an empty table (no current snapshot) returns 0/0 without touching the executor");
    }

    @Test
    public void combinesPerAppendManifests() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        // Three separate appends -> three data manifests accumulate in the current snapshot.
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        Table before = catalog.loadTable(id);
        int manifestsBefore = before.currentSnapshot().dataManifests(before.io()).size();
        Assertions.assertEquals(3, manifestsBefore);

        IcebergRewriteManifestsAction action = action(Collections.emptyMap());
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        // Iceberg rewrites nothing without a cluster function (BaseRewriteManifests.requiresRewrite) and only
        // commits a snapshot that keeps every manifest. With one, the three manifests of an unpartitioned table
        // become one, and the counts are the manifests actually replaced and written.
        Assertions.assertEquals(ImmutableList.of("3", "1"), result.getRows().get(0));
        Table after = catalog.loadTable(id);
        Assertions.assertEquals(1, after.currentSnapshot().dataManifests(after.io()).size());
        Assertions.assertEquals(3, Iterables.size(after.newScan().planFiles()), "every data file is kept");
    }

    @Test
    public void mergesPartitionedManifestsThatFitTheTargetSizeIntoOne() {
        // A partition spec gets min(ceil(size of its manifests / target size), 200) clusters, as in Trino's
        // optimize_manifests. Four small manifests fit in one target-size manifest, so they become one.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(ActionTestTables.SCHEMA).identity("id").identity("name")
                .build();
        ActionTestTables.createTable(catalog, "t", spec);
        append(catalog, partitionedFile(spec, "f1.parquet", 1, "a"));
        append(catalog, partitionedFile(spec, "f2.parquet", 1, "b"));
        append(catalog, partitionedFile(spec, "f3.parquet", 2, "a"));
        append(catalog, partitionedFile(spec, "f4.parquet", 2, "b"));

        ConnectorProcedureResult result = rewriteAll(catalog);

        Assertions.assertEquals(ImmutableList.of("4", "1"), result.getRows().get(0));
        Assertions.assertEquals(Collections.singletonList(new HashSet<>(Arrays.asList(1, 2))),
                firstPartitionValuesPerManifest(catalog));
    }

    @Test
    public void clustersAdjacentFirstPartitionValues() {
        // With two clusters, the sorted values 1, 7, 50 and 100 go to runs {1, 7} and {50, 100}, so each manifest
        // covers a narrow range of the first partition field. The files are appended out of order, and a HashSet
        // of these Integers iterates 1, 50, 100, 7, so clustering the values unsorted would mix the runs.
        // MUTATION: not sorting the values -> red.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(ActionTestTables.SCHEMA).identity("id").identity("name")
                .build();
        ActionTestTables.createTable(catalog, "t", spec);
        append(catalog, partitionedFile(spec, "f1.parquet", 100, "a"));
        append(catalog, partitionedFile(spec, "f2.parquet", 1, "b"));
        append(catalog, partitionedFile(spec, "f3.parquet", 50, "a"));
        append(catalog, partitionedFile(spec, "f4.parquet", 7, "b"));
        targetClusters(catalog, 2);

        ConnectorProcedureResult result = rewriteAll(catalog);

        Assertions.assertEquals(ImmutableList.of("4", "2"), result.getRows().get(0));
        Assertions.assertEquals(
                Arrays.asList(new HashSet<>(Arrays.asList(1, 7)), new HashSet<>(Arrays.asList(50, 100))),
                firstPartitionValuesPerManifest(catalog));
    }

    @Test
    public void clustersPartitionsReadFromTheSameManifest() {
        // One commit can write several partitions into one manifest: each commit below writes both, and with two
        // clusters the rewrite splits them by partition value.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(ActionTestTables.SCHEMA).identity("id").build();
        ActionTestTables.createTable(catalog, "t", spec);
        append(catalog, partitionedFile(spec, "f1.parquet", 1), partitionedFile(spec, "f2.parquet", 2));
        append(catalog, partitionedFile(spec, "f3.parquet", 1), partitionedFile(spec, "f4.parquet", 2));
        targetClusters(catalog, 2);

        ConnectorProcedureResult result = rewriteAll(catalog);

        Assertions.assertEquals(ImmutableList.of("2", "2"), result.getRows().get(0));
        assertOneFirstPartitionValuePerManifest(catalog, Arrays.asList(1, 2));
    }

    @Test
    public void clustersANullFirstPartitionValue() {
        // A NULL partition value sorts first, so with two clusters it takes the first and 1 the second. Iceberg
        // rejects a null cluster key, and the cluster of a NULL value is a number like any other.
        // MUTATION: sorting without NULLs first -> red (the comparator does not take a null).
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(ActionTestTables.SCHEMA).identity("id").identity("name")
                .build();
        ActionTestTables.createTable(catalog, "t", spec);
        append(catalog, partitionedFile(spec, "f1.parquet", null, "a"));
        append(catalog, partitionedFile(spec, "f2.parquet", null, "b"));
        append(catalog, partitionedFile(spec, "f3.parquet", 1, "a"));
        targetClusters(catalog, 2);

        ConnectorProcedureResult result = rewriteAll(catalog);

        Assertions.assertEquals(ImmutableList.of("3", "2"), result.getRows().get(0));
        assertOneFirstPartitionValuePerManifest(catalog, Arrays.asList(null, 1));
    }

    @Test
    public void clustersEachPartitionSpecByItsOwnFirstField() {
        // After the spec changed from identity(id) to identity(name), the manifests of each spec are clustered by
        // that spec's own first field, an int in one and a string in the other: one comparator for all the values
        // would fail on the mix. The files are fast-appended, since a merge append would also merge the manifests
        // of the old spec. MUTATION: clustering every spec with the current spec's first field -> red.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        PartitionSpec idSpec = PartitionSpec.builderFor(ActionTestTables.SCHEMA).identity("id").build();
        ActionTestTables.createTable(catalog, "t", idSpec);
        catalog.loadTable(id).newFastAppend().appendFile(partitionedFile(idSpec, "f1.parquet", 1)).commit();
        catalog.loadTable(id).newFastAppend().appendFile(partitionedFile(idSpec, "f2.parquet", 2)).commit();
        catalog.loadTable(id).updateSpec().removeField("id").addField("name").commit();
        PartitionSpec nameSpec = catalog.loadTable(id).spec();
        catalog.loadTable(id).newFastAppend().appendFile(partitionedFile(nameSpec, "f3.parquet", "a")).commit();
        catalog.loadTable(id).newFastAppend().appendFile(partitionedFile(nameSpec, "f4.parquet", "b")).commit();
        targetClusters(catalog, 4);

        ConnectorProcedureResult result = rewriteAll(catalog);

        Assertions.assertEquals(ImmutableList.of("4", "4"), result.getRows().get(0));
        assertOneFirstPartitionValuePerManifest(catalog, Arrays.asList(1, 2, "a", "b"));
    }

    @Test
    public void failsBeforeCommitWhenTheFirstPartitionFieldIsPromotedDuringTheRewrite() {
        // Another writer promotes the identity source column from INT to LONG after the table was loaded. Iceberg
        // rebuilds the existing specs with the new type under the same ids, so the rewrite reads the targeted
        // manifests' values as Longs while the values read ahead were Integers. The rewrite fails before it
        // commits, and the table is left as it was.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        PartitionSpec spec = PartitionSpec.builderFor(ActionTestTables.SCHEMA).identity("id").build();
        ActionTestTables.createTable(catalog, "t", spec);
        catalog.loadTable(id).newFastAppend().appendFile(partitionedFile(spec, "f1.parquet", 1)).commit();
        catalog.loadTable(id).newFastAppend().appendFile(partitionedFile(spec, "f2.parquet", 2)).commit();
        targetClusters(catalog, 2);
        Table loaded = catalog.loadTable(id);
        catalog.loadTable(id).updateSchema().updateColumn("id", org.apache.iceberg.types.Types.LongType.get())
                .commit();
        Set<String> manifestsBefore = manifestPaths(catalog.loadTable(id));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, () -> rewrite(loaded));

        Assertions.assertTrue(e.getMessage().contains("the table's partitioning changed during the rewrite"),
                e.getMessage());
        Table after = catalog.loadTable(id);
        Assertions.assertEquals(loaded.currentSnapshot().snapshotId(), after.currentSnapshot().snapshotId());
        Assertions.assertEquals(manifestsBefore, manifestPaths(after));
    }

    @Test
    public void clusterCountIsTheManifestSizeOverTheTargetUpTo200() {
        // As in Trino, at most 200 clusters, and so at most 200 open manifest writers, per spec; the division
        // rounds up without overflowing. MUTATION: no cap, or rounding up as (size + target - 1) / target -> red.
        long target = 8L * 1024 * 1024;
        Assertions.assertEquals(1, RewriteManifestExecutor.clusterCount(1, target));
        Assertions.assertEquals(1, RewriteManifestExecutor.clusterCount(target, target));
        Assertions.assertEquals(2, RewriteManifestExecutor.clusterCount(target + 1, target));
        Assertions.assertEquals(200, RewriteManifestExecutor.clusterCount(200 * target, target));
        Assertions.assertEquals(200, RewriteManifestExecutor.clusterCount(Long.MAX_VALUE, target));
        Assertions.assertEquals(3, RewriteManifestExecutor.clusterCount(Long.MAX_VALUE, Long.MAX_VALUE / 2));
    }

    @Test
    public void assignsSortedValuesToRunsOfNearlyEqualLength() {
        // 1000 values in 200 clusters: five adjacent values per cluster, NULL first. Three values in 200 clusters
        // are three clusters.
        Comparator<Object> order = Comparators.<Object>nullsFirst().thenComparing(
                Comparators.forType(org.apache.iceberg.types.Types.IntegerType.get()));
        Set<Object> values = new HashSet<>();
        values.add(null);
        for (int i = 0; i < 999; i++) {
            values.add(i);
        }

        Map<Object, Integer> clusters = RewriteManifestExecutor.assignClusters(values, order, 200);

        Assertions.assertEquals(0, clusters.get(null).intValue());
        int[] sizes = new int[200];
        int previous = clusters.get(null);
        sizes[previous]++;
        for (int i = 0; i < 999; i++) {
            int cluster = clusters.get(i);
            Assertions.assertTrue(cluster >= previous, "cluster numbers follow the sort order");
            sizes[cluster]++;
            previous = cluster;
        }
        for (int size : sizes) {
            Assertions.assertEquals(5, size);
        }
        Assertions.assertEquals(3, new HashSet<>(RewriteManifestExecutor.assignClusters(
                new HashSet<>(Arrays.asList(3, 1, 2)), order, 200).values()).size());
    }

    @Test
    public void leavesASingleSmallManifestUntouched() {
        // One manifest below commit.manifest.target-size-bytes has nothing to merge with, as in Spark's and
        // Trino's manifest rewrites: nothing is committed and nothing is counted.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        long snapshotBefore = catalog.loadTable(id).currentSnapshot().snapshotId();

        ConnectorProcedureResult result = rewriteAll(catalog);

        Assertions.assertEquals(ImmutableList.of("0", "0"), result.getRows().get(0));
        Assertions.assertEquals(snapshotBefore, catalog.loadTable(id).currentSnapshot().snapshotId(),
                "no snapshot is committed");
    }

    @Test
    public void keepsAManifestCommittedAfterTheTableWasLoaded() {
        // The rewrite targets the manifests of the snapshot the table was loaded with. A manifest that another
        // writer commits before the rewrite applies is kept as it is, and the counts are those of the snapshot
        // the rewrite committed, which the table's manifests before and after it would not give.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        Table loaded = catalog.loadTable(id);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        Set<String> concurrentManifests = manifestPaths(catalog.loadTable(id));
        concurrentManifests.removeAll(manifestPaths(loaded));
        Assertions.assertEquals(1, concurrentManifests.size());

        ConnectorProcedureResult result = rewrite(loaded);

        Assertions.assertEquals(ImmutableList.of("2", "1"), result.getRows().get(0));
        Table after = catalog.loadTable(id);
        Set<String> manifestsAfter = manifestPaths(after);
        Assertions.assertEquals(2, manifestsAfter.size(), manifestsAfter.toString());
        Assertions.assertTrue(manifestsAfter.containsAll(concurrentManifests), manifestsAfter.toString());
        Assertions.assertEquals(3, Iterables.size(after.newScan().planFiles()), "every data file is kept");
    }

    @Test
    public void keepsAManifestOfAPartitionSpecAddedAfterTheTableWasLoaded() {
        // Iceberg's RewriteManifests takes the partition specs of the table it is created from, so it cannot
        // rewrite a manifest of a spec that another writer adds after the table was loaded: a rewrite that
        // reached it failed with a NullPointerException. That manifest is kept as it is. The other writer fast
        // appends, because a merge append would also merge the manifests of the old spec.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        Table loaded = catalog.loadTable(id);
        catalog.loadTable(id).updateSpec().addField("id").commit();
        Table evolved = catalog.loadTable(id);
        evolved.newFastAppend().appendFile(partitionedFile(evolved.spec(), "f3.parquet", 3)).commit();

        ConnectorProcedureResult result = rewrite(loaded);

        Assertions.assertEquals(ImmutableList.of("2", "1"), result.getRows().get(0));
        Table after = catalog.loadTable(id);
        List<Integer> specIds = new ArrayList<>();
        for (ManifestFile manifest : after.currentSnapshot().dataManifests(after.io())) {
            specIds.add(manifest.partitionSpecId());
        }
        Collections.sort(specIds);
        Assertions.assertEquals(Arrays.asList(0, 1), specIds);
        Assertions.assertEquals(3, Iterables.size(after.newScan().planFiles()), "every data file is kept");
    }

    private static ConnectorProcedureResult rewriteAll(InMemoryCatalog catalog) {
        return rewrite(catalog.loadTable(ActionTestTables.id("t")));
    }

    private static ConnectorProcedureResult rewrite(Table table) {
        IcebergRewriteManifestsAction action = action(Collections.emptyMap());
        action.validate();
        return action.execute(table, ActionTestTables.session("UTC"));
    }

    private static Set<String> manifestPaths(Table table) {
        Set<String> paths = new HashSet<>();
        for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
            paths.add(manifest.path());
        }
        return paths;
    }

    private static DataFile partitionedFile(PartitionSpec spec, String fileName, Object... partitionValues) {
        PartitionData partition = new PartitionData(spec.partitionType());
        for (int i = 0; i < partitionValues.length; i++) {
            partition.set(i, partitionValues[i]);
        }
        return DataFiles.builder(spec)
                .withPath("s3://b/db1/" + fileName)
                .withFileSizeInBytes(1024)
                .withRecordCount(1)
                .withFormat(FileFormat.PARQUET)
                .withPartition(partition)
                .build();
    }

    private static void append(InMemoryCatalog catalog, DataFile... files) {
        AppendFiles append = catalog.loadTable(ActionTestTables.id("t")).newAppend();
        for (DataFile file : files) {
            append.appendFile(file);
        }
        append.commit();
    }

    /**
     * Sets the target manifest size so that t's data manifests make the given number of clusters: each manifest
     * is a few KB, so ceil(total / ceil(total / clusters)) is clusters, and a rewritten manifest, which holds one
     * header for the files of several, stays below the target.
     */
    private static void targetClusters(InMemoryCatalog catalog, int clusters) {
        Table table = catalog.loadTable(ActionTestTables.id("t"));
        long total = 0;
        for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
            total += manifest.length();
        }
        table.updateProperties()
                .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf((total + clusters - 1) / clusters))
                .commit();
    }

    /** The first-partition-field values of each data manifest of t, ordered by the manifests' smallest values. */
    private static List<Set<Object>> firstPartitionValuesPerManifest(InMemoryCatalog catalog) {
        Table table = catalog.loadTable(ActionTestTables.id("t"));
        List<Set<Object>> manifests = new ArrayList<>();
        for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
            Set<Object> manifestValues = new HashSet<>();
            try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, table.io(), table.specs())) {
                for (DataFile file : files) {
                    manifestValues.add(file.partition().get(0, Object.class));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            manifests.add(manifestValues);
        }
        manifests.sort(Comparator.comparingInt((Set<Object> values) -> values.stream()
                .mapToInt(value -> (Integer) value).min().getAsInt()));
        return manifests;
    }

    /** Every data manifest holds files of one first-partition-field value, and together they hold expected. */
    private static void assertOneFirstPartitionValuePerManifest(InMemoryCatalog catalog, List<?> expected) {
        Table table = catalog.loadTable(ActionTestTables.id("t"));
        List<Object> values = new ArrayList<>();
        for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
            Set<Object> manifestValues = new HashSet<>();
            try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, table.io(), table.specs())) {
                for (DataFile file : files) {
                    manifestValues.add(file.partition().get(0, Object.class));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            Assertions.assertEquals(1, manifestValues.size(), manifest.path() + " holds " + manifestValues);
            values.addAll(manifestValues);
        }
        Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(values));
        Assertions.assertEquals(expected.size(), values.size(), "one manifest per first partition value");
    }

    @Test
    public void resultSchemaIsTwoInts() {
        Assertions.assertEquals(2, action(Collections.emptyMap()).getResultSchema().size());
        Assertions.assertEquals("rewritten_manifests_count",
                action(Collections.emptyMap()).getResultSchema().get(0).getName());
        Assertions.assertEquals("added_manifests_count",
                action(Collections.emptyMap()).getResultSchema().get(1).getName());
        Assertions.assertEquals("INT",
                action(Collections.emptyMap()).getResultSchema().get(0).getType().getTypeName());
        Assertions.assertEquals("INT",
                action(Collections.emptyMap()).getResultSchema().get(1).getType().getTypeName());
        Assertions.assertFalse(action(Collections.emptyMap()).getResultSchema().get(0).isNullable());
        Assertions.assertFalse(action(Collections.emptyMap()).getResultSchema().get(1).isNullable());
    }

    @Test
    public void specIdAcceptsZeroToMaxRange() {
        // spec_id is optional with intRange(0, MAX); a negative value is rejected at parse time.
        Assertions.assertDoesNotThrow(() -> action(ImmutableMap.of("spec_id", "0")).validate());
    }

    @Test
    public void specIdFiltersManifestsByPartitionSpec() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        // getInt(SPEC_ID) reads parsedValues populated by validate(), so validate() MUST run before execute()
        // for the spec_id filter to take effect (skipping it makes spec_id silently a no-op). Run the
        // non-matching case FIRST: all manifests are spec 0, so spec_id=1 targets zero -> the executor
        // short-circuits at rewrittenCount==0 to ["0","0"] WITHOUT committing, leaving the table pristine for
        // the matching case below.
        IcebergRewriteManifestsAction miss = action(ImmutableMap.of("spec_id", "1"));
        miss.validate();
        ConnectorProcedureResult unmatched = miss.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));
        Assertions.assertEquals(ImmutableList.of("0", "0"), unmatched.getRows().get(0),
                "spec_id=1 matches none of the (all spec-0) manifests -> rewrittenCount==0 short-circuit");
        // spec_id=0 matches the unpartitioned spec -> all three manifests are targeted for rewrite.
        IcebergRewriteManifestsAction keep = action(ImmutableMap.of("spec_id", "0"));
        keep.validate();
        ConnectorProcedureResult kept = keep.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));
        Assertions.assertEquals("3", kept.getRows().get(0).get(0),
                "spec_id=0 targets every manifest's partitionSpecId()==0");
    }

    @Test
    public void wrapsCurrentSnapshotFailure() {
        // In the connector port, executeAction probes icebergTable.currentSnapshot() itself (before the
        // executor); a throw there is caught and wrapped ONCE by the action's "Rewrite manifests failed: "
        // handler. (The executor's own "Failed to rewrite manifests: " prefix is not reached on this path,
        // because the action short-circuits the empty/throwing snapshot before constructing the executor.)
        Table throwing = new ThrowingTable();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(Collections.emptyMap()).execute(throwing, ActionTestTables.session("UTC")));
        Assertions.assertEquals("Rewrite manifests failed: boom", e.getMessage());
    }

    @Test
    public void executorWrapsFailureWithInnerPrefix() {
        // The other half of the double-wrap quirk: RewriteManifestExecutor.execute probes
        // table.currentSnapshot() first inside its OWN try, so a throw there is wrapped with the executor's
        // inner "Failed to rewrite manifests: " prefix. Composed with the action's outer "Rewrite manifests
        // failed: " wrap (wrapsCurrentSnapshotFailure), this pins BOTH layers of the byte string the action
        // emits when the executor itself fails ("Rewrite manifests failed: Failed to rewrite manifests: ...").
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> new RewriteManifestExecutor().execute(new ThrowingTable(), null));
        Assertions.assertEquals("Failed to rewrite manifests: boom", e.getMessage());
    }

    /**
     * Minimal {@link Table} double whose {@link #currentSnapshot()} throws — the first probe inside the
     * action's {@code executeAction}. The action body wraps it as "Rewrite manifests failed: boom".
     * {@link #name()} is read only by the LOG.warn call so it must not throw; every other method is outside
     * this path and fails loud.
     */
    private static final class ThrowingTable implements Table {

        @Override
        public String name() {
            return "throwing";
        }

        @Override
        public Snapshot currentSnapshot() {
            throw new RuntimeException("boom");
        }

        @Override
        public void refresh() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableScan newScan() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Schema schema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, Schema> schemas() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PartitionSpec spec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, PartitionSpec> specs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortOrder sortOrder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, SortOrder> sortOrders() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> properties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String location() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Snapshot> snapshots() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HistoryEntry> history() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdateSchema updateSchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdatePartitionSpec updateSpec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdateProperties updateProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReplaceSortOrder replaceSortOrder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdateLocation updateLocation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AppendFiles newAppend() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RewriteFiles newRewrite() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RewriteManifests rewriteManifests() {
            throw new UnsupportedOperationException();
        }

        @Override
        public OverwriteFiles newOverwrite() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowDelta newRowDelta() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReplacePartitions newReplacePartitions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeleteFiles newDelete() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpireSnapshots expireSnapshots() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ManageSnapshots manageSnapshots() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Transaction newTransaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIO io() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EncryptionManager encryption() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocationProvider locationProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<StatisticsFile> statisticsFiles() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, SnapshotRef> refs() {
            throw new UnsupportedOperationException();
        }
    }
}
