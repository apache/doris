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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Executor for manifest rewrite operations. Connector port of legacy
 * {@code datasource/iceberg/rewrite/RewriteManifestExecutor}. The fe-core couplings are dropped: there is no
 * {@code ExternalTable} parameter and no {@code Env.getExtMetaCacheMgr().invalidateTableCache} call (cache
 * invalidation is performed once at dispatch level by {@code IcebergProcedureOps}); the failure message is kept
 * byte-identical to the legacy {@code UserException}. Unlike the legacy executor, which committed a rewrite
 * without a cluster function (so Iceberg kept every manifest as it was), it clusters the data files by runs of
 * adjacent values of their first partition field, skips a single manifest below the target size, and reports the
 * manifests the rewrite actually replaced and wrote.
 */
public class RewriteManifestExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteManifestExecutor.class);

    /**
     * The most clusters of one partition spec, and so the most manifest writers that a rewrite keeps open for it,
     * as in Trino's optimize_manifests: Iceberg's clusterBy keeps one writer per key open until the scan ends.
     */
    private static final int MAX_CLUSTERS_PER_SPEC = 200;

    public static class Result {
        private final int rewrittenCount;
        private final int addedCount;

        public Result(int rewrittenCount, int addedCount) {
            this.rewrittenCount = rewrittenCount;
            this.addedCount = addedCount;
        }

        public java.util.List<String> toStringList() {
            return java.util.Arrays.asList(String.valueOf(rewrittenCount),
                    String.valueOf(addedCount));
        }
    }

    /**
     * Execute manifest rewrite using Iceberg RewriteManifests API
     */
    public Result execute(Table table, Integer specId) {
        try {
            // Get current snapshot and return early if table is empty
            Snapshot currentSnapshot = table.currentSnapshot();
            if (currentSnapshot == null) {
                return new Result(0, 0);
            }

            // Collect manifests before rewrite and filter by specId if provided
            List<ManifestFile> manifestsBeforeTargeted =
                    filterBySpecId(currentSnapshot.dataManifests(table.io()), specId);
            if (manifestsBeforeTargeted.isEmpty()) {
                return new Result(0, 0);
            }
            // One manifest below the target size has nothing to merge with, as in Spark's and Trino's manifest
            // rewrites: leave the table alone instead of committing a snapshot that changes nothing.
            long targetManifestSizeBytes = PropertyUtil.propertyAsLong(table.properties(),
                    TableProperties.MANIFEST_TARGET_SIZE_BYTES, TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
            if (manifestsBeforeTargeted.size() == 1
                    && manifestsBeforeTargeted.get(0).length() < targetManifestSizeBytes) {
                return new Result(0, 0);
            }

            // Without a cluster function Iceberg rewrites nothing (BaseRewriteManifests.requiresRewrite) and only
            // commits a snapshot that keeps every manifest. Only the targeted manifests are rewritten: a manifest
            // that another writer commits before the rewrite applies is kept as it is. Such a manifest can be of a
            // partition spec added after the table was loaded, which the rewrite cannot write, because it takes
            // the specs of the table it is created from.
            Set<String> targetedPaths = manifestsBeforeTargeted.stream()
                    .map(ManifestFile::path)
                    .collect(Collectors.toSet());
            Map<Integer, Map<Object, Integer>> clustersBySpec =
                    clustersBySpec(table, manifestsBeforeTargeted, targetManifestSizeBytes);
            RewriteManifests rm = table.rewriteManifests()
                    .clusterBy(file -> clusterOf(clustersBySpec, file))
                    .rewriteIf(manifest -> targetedPaths.contains(manifest.path()));
            rm.commit();

            // Count the manifests replaced and written by the snapshot this rewrite committed. Iceberg builds the
            // event of that snapshot after the commit (SnapshotProducer.notifyListeners), while the table's current
            // snapshot can already be another one.
            Map<String, String> summary = ((CreateSnapshotEvent) rm.updateEvent()).summary();
            return new Result(Integer.parseInt(summary.get(SnapshotSummary.REPLACED_MANIFESTS_COUNT)),
                    Integer.parseInt(summary.get(SnapshotSummary.CREATED_MANIFESTS_COUNT)));
        } catch (Exception e) {
            LOG.warn("Failed to execute manifest rewrite for table: {}", table.name(), e);
            throw new DorisConnectorException("Failed to rewrite manifests: " + e.getMessage(), e);
        }
    }

    /**
     * Groups the data files of each targeted partition spec into runs of adjacent values of the spec's first
     * partition field, as Trino's optimize_manifests does. A spec gets min(ceil(size of its targeted manifests /
     * target size), 200) clusters: the distinct values are read from its targeted manifests, sorted with NULL first,
     * and the value at rank i goes to cluster i * clusters / n. Adjacent partitions then share a manifest, which
     * keeps the manifests' partition summaries narrow for pruning, while the rewrite keeps at most that many
     * writers open for the spec; Iceberg still rolls each cluster over at the target size. An unpartitioned spec, or
     * one whose targeted manifests fit in one target-size manifest, is one cluster and is not read ahead. Each spec
     * is clustered on its own first field, which can be of another type than another spec's: Iceberg keeps a writer
     * per cluster and spec, so the specs' cluster numbers do not collide.
     *
     * @return the value-to-cluster map of each targeted spec, empty for a spec that is one cluster
     */
    private static Map<Integer, Map<Object, Integer>> clustersBySpec(Table table, List<ManifestFile> targeted,
            long targetManifestSizeBytes) throws IOException {
        Map<Integer, List<ManifestFile>> manifestsBySpec = targeted.stream()
                .collect(Collectors.groupingBy(ManifestFile::partitionSpecId));
        Map<Integer, Map<Object, Integer>> clustersBySpec = new HashMap<>();
        for (Map.Entry<Integer, List<ManifestFile>> entry : manifestsBySpec.entrySet()) {
            PartitionSpec spec = table.specs().get(entry.getKey());
            int clusters = clusterCount(entry.getValue().stream().mapToLong(ManifestFile::length).sum(),
                    targetManifestSizeBytes);
            if (spec.isUnpartitioned() || clusters == 1) {
                clustersBySpec.put(entry.getKey(), Collections.emptyMap());
                continue;
            }
            Comparator<Object> order = Comparators.<Object>nullsFirst().thenComparing(
                    Comparators.forType(spec.partitionType().fields().get(0).type().asPrimitiveType()));
            clustersBySpec.put(entry.getKey(),
                    assignClusters(firstPartitionValues(table, entry.getValue()), order, clusters));
        }
        return clustersBySpec;
    }

    /** min(ceil(manifestsSizeBytes / targetManifestSizeBytes), 200); a manifest is never empty, so at least 1. */
    static int clusterCount(long manifestsSizeBytes, long targetManifestSizeBytes) {
        return (int) Math.min((manifestsSizeBytes - 1) / targetManifestSizeBytes + 1, MAX_CLUSTERS_PER_SPEC);
    }

    /**
     * Sorts the values and gives the one at rank i cluster i * clusters / n, so each cluster is a run of adjacent
     * values and the runs differ in length by at most one.
     */
    static Map<Object, Integer> assignClusters(Set<Object> values, Comparator<Object> order, int clusters) {
        List<Object> sorted = new ArrayList<>(values);
        sorted.sort(order);
        Map<Object, Integer> clusterOfValue = new HashMap<>();
        for (int rank = 0; rank < sorted.size(); rank++) {
            clusterOfValue.put(sorted.get(rank), (int) ((long) rank * clusters / sorted.size()));
        }
        return Collections.unmodifiableMap(clusterOfValue);
    }

    /**
     * The distinct values of the first partition field among the live data files of manifests of one spec, read
     * with only the partition column projected, in parallel on Iceberg's worker pool as the rewrite itself reads
     * them. ManifestReader returns copies of the files, and PartitionData returns immutable values or copies of
     * binary ones, so the values can be kept and compared by content; a HashSet holds a NULL value as well.
     */
    private static Set<Object> firstPartitionValues(Table table, List<ManifestFile> manifests) throws IOException {
        Iterable<ManifestReader<DataFile>> readers = Iterables.transform(manifests,
                manifest -> ManifestFiles.read(manifest, table.io(), table.specs())
                        .select(Collections.singletonList("partition")));
        Set<Object> values = new HashSet<>();
        try (CloseableIterable<DataFile> files = new ParallelIterable<>(readers, ThreadPools.getWorkerPool())) {
            for (DataFile file : files) {
                values.add(file.partition().get(0, Object.class));
            }
        }
        return values;
    }

    /**
     * The cluster of a data file. Iceberg calls this from its worker threads while it reads the targeted manifests,
     * so it only reads maps built before the commit. Every file it is given comes from a targeted manifest, whose
     * values were read ahead; a value missing from the map means the table's partitioning changed while the
     * rewrite ran.
     */
    private static Object clusterOf(Map<Integer, Map<Object, Integer>> clustersBySpec, DataFile file) {
        Map<Object, Integer> clusters = clustersBySpec.get(file.specId());
        if (clusters.isEmpty()) {
            return 0;
        }
        Object value = file.partition().get(0, Object.class);
        Integer cluster = clusters.get(value);
        Preconditions.checkState(cluster != null, "Partition value %s of %s was not in the targeted manifests when"
                + " the rewrite started; the table's partitioning changed during the rewrite", value, file.location());
        return cluster;
    }

    private List<ManifestFile> filterBySpecId(List<ManifestFile> manifests, Integer specId) {
        if (specId == null) {
            return manifests;
        }
        final int targetSpecId = specId;
        return manifests.stream()
                .filter(manifest -> manifest.partitionSpecId() == targetSpecId)
                .collect(Collectors.toList());
    }
}
