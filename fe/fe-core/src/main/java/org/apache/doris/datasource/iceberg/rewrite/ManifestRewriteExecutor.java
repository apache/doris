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
import org.apache.doris.datasource.ExternalTable;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Executor for manifest rewrite operations
 */
public class ManifestRewriteExecutor {
    private static final Logger LOG = LogManager.getLogger(ManifestRewriteExecutor.class);

    public static class Result {
        private final int rewrittenCount;
        private final int totalCount;

        public Result(int rewrittenCount, int totalCount) {
            this.rewrittenCount = rewrittenCount;
            this.totalCount = totalCount;
        }

        public java.util.List<String> toStringList() {
            return java.util.Arrays.asList(String.valueOf(rewrittenCount),
                    String.valueOf(totalCount));
        }
    }

    /**
     * Execute manifest rewrite using Iceberg RewriteManifests API
     */
    public Result execute(Table table, ExternalTable extTable,
                          boolean clusterByPartition, int scanThreads,
                          Predicate<ManifestFile> predicate) {
        ExecutorService executor = null;
        try {
            Snapshot currentSnapshot = table.currentSnapshot();
            if (currentSnapshot == null) {
                return new Result(0, 0);
            }

            // Get manifest statistics before rewrite
            List<ManifestFile> dataManifests = currentSnapshot.dataManifests(table.io());
            int totalManifests = dataManifests.size();
            int selectedManifests = (int) dataManifests.stream()
                    .filter(predicate)
                    .count();

            // Execute rewrite operation
            RewriteManifests rm = table.rewriteManifests();

            // Optional: cluster by partition
            if (clusterByPartition) {
                rm.clusterBy(ContentFile::partition);
            }

            // Optional: use parallel scanning
            if (scanThreads > 0) {
                executor = Executors.newFixedThreadPool(scanThreads);
                rm.scanManifestsWith(executor);
            }

            // Execute rewrite based on predicate
            rm.rewriteIf(predicate).commit();

            // Invalidate cache
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(extTable);
            return new Result(selectedManifests, totalManifests);
        } finally {
            if (executor != null) {
                shutdownExecutor(executor);
            }
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        // Disable new tasks from being submitted
        executor.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    // Log warning if executor doesn't terminate
                    LOG.warn("ExecutorService did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
