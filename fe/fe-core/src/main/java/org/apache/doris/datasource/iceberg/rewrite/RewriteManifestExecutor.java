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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Executor for manifest rewrite operations
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
    public Result execute(Table table, ExternalTable extTable, Integer specId) throws UserException {
        try {
            // Get current snapshot and return early if table is empty
            Snapshot currentSnapshot = table.currentSnapshot();
            if (currentSnapshot == null) {
                return new Result(0, 0);
            }

            // Collect manifests before rewrite and filter by specId if provided
            List<ManifestFile> manifestsBefore = currentSnapshot.dataManifests(table.io());
            List<ManifestFile> manifestsBeforeTargeted = filterBySpecId(manifestsBefore, specId);

            int rewrittenCount = manifestsBeforeTargeted.size();

            if (rewrittenCount == 0) {
                return new Result(0, 0);
            }

            // Configure rewrite operation, optionally restricting manifests by specId
            RewriteManifests rm = table.rewriteManifests();

            if (specId != null) {
                final int targetSpecId = specId;
                rm.rewriteIf(manifest -> manifest.partitionSpecId() == targetSpecId);
            }

            // Commit manifest rewrite
            rm.commit();

            // Refresh snapshot after rewrite
            Snapshot snapshotAfter = table.currentSnapshot();
            if (snapshotAfter == null) {
                return new Result(rewrittenCount, 0);
            }

            // Collect manifests after rewrite and filter by specId
            List<ManifestFile> manifestsAfter = snapshotAfter.dataManifests(table.io());
            List<ManifestFile> manifestsAfterTargeted = filterBySpecId(manifestsAfter, specId);

            // Compute addedCount as newly produced manifests (path not in before set)
            java.util.Set<String> beforePaths = manifestsBeforeTargeted.stream()
                    .map(ManifestFile::path)
                    .collect(java.util.stream.Collectors.toSet());

            int addedCount = (int) manifestsAfterTargeted.stream()
                    .map(ManifestFile::path)
                    .filter(path -> !beforePaths.contains(path))
                    .count();

            // Invalidate table cache to ensure metadata is refreshed
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(extTable);

            return new Result(rewrittenCount, addedCount);
        } catch (Exception e) {
            LOG.warn("Failed to execute manifest rewrite for table: {}", extTable.getName(), e);
            throw new UserException("Failed to rewrite manifests: " + e.getMessage(), e);
        }
    }

    private List<ManifestFile> filterBySpecId(List<ManifestFile> manifests, Integer specId) {
        if (specId == null) {
            return manifests;
        }
        final int targetSpecId = specId;
        return manifests.stream()
                .filter(manifest -> manifest.partitionSpecId() == targetSpecId)
                .collect(java.util.stream.Collectors.toList());
    }
}
