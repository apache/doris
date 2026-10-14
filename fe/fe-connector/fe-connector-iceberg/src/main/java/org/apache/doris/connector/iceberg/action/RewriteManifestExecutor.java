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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
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
 * without a cluster function (so Iceberg kept every manifest as it was), it clusters the data files by their
 * first partition field, skips a single manifest below the target size, and reports the manifests the rewrite
 * actually replaced and wrote.
 */
public class RewriteManifestExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteManifestExecutor.class);

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
            RewriteManifests rm = table.rewriteManifests()
                    .clusterBy(RewriteManifestExecutor::clusterKey)
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
     * Groups data files by the value of their first partition field (one group for an unpartitioned table), as
     * Trino's optimize_manifests does, so a rewrite of a multi-field partition does not write one manifest per
     * partition. The key holds the value PartitionData returns (immutable, or a copy for binary values) rather
     * than the partition, so it does not depend on whether ManifestReader reuses the partition between rows (its
     * iterator() copies files for that reason); the list lets a NULL value be a key, which Iceberg rejects.
     */
    private static Object clusterKey(DataFile file) {
        StructLike partition = file.partition();
        return partition.size() == 0
                ? Collections.emptyList()
                : Collections.singletonList(partition.get(0, Object.class));
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
