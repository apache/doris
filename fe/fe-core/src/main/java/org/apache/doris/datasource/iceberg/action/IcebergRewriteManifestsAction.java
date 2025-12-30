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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.rewrite.ManifestRewriteExecutor;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Action for rewriting Iceberg manifest files to optimize metadata layout
 */
public class IcebergRewriteManifestsAction extends BaseIcebergAction {
    private static final Logger LOG = LogManager.getLogger(IcebergRewriteManifestsAction.class);
    public static final String CLUSTER_BY_PARTITION = "cluster-by-partition";
    public static final String REWRITE_ALL = "rewrite-all";
    public static final String MIN_MANIFEST_SIZE_BYTES = "min-manifest-size-bytes";
    public static final String MAX_MANIFEST_SIZE_BYTES = "max-manifest-size-bytes";
    public static final String SCAN_THREAD_POOL_SIZE = "scan-thread-pool-size";

    public IcebergRewriteManifestsAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super("rewrite_manifests", properties, partitionNamesInfo, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerOptionalArgument(CLUSTER_BY_PARTITION,
                "Cluster manifests by partition fields",
                true,
                ArgumentParsers.booleanValue(CLUSTER_BY_PARTITION));

        namedArguments.registerOptionalArgument(REWRITE_ALL,
                "Rewrite all manifests when true; otherwise use size thresholds",
                true,
                ArgumentParsers.booleanValue(REWRITE_ALL));

        namedArguments.registerOptionalArgument(MIN_MANIFEST_SIZE_BYTES,
                "Minimum manifest file size to be considered for rewrite",
                0L,
                ArgumentParsers.positiveLong(MIN_MANIFEST_SIZE_BYTES));

        namedArguments.registerOptionalArgument(MAX_MANIFEST_SIZE_BYTES,
                "Maximum manifest file size to be considered for rewrite",
                0L,
                ArgumentParsers.positiveLong(MAX_MANIFEST_SIZE_BYTES));

        namedArguments.registerOptionalArgument(SCAN_THREAD_POOL_SIZE,
                "Thread pool size for parallel manifest scanning",
                0,
                ArgumentParsers.intRange(SCAN_THREAD_POOL_SIZE, 0, 16));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        validateNoPartitions();
        validateNoWhereCondition();

        // Validate size parameter relationships
        long minSize = namedArguments.getLong(MIN_MANIFEST_SIZE_BYTES);
        long maxSize = namedArguments.getLong(MAX_MANIFEST_SIZE_BYTES);

        if (maxSize > 0 && minSize > maxSize) {
            throw new UserException("min-manifest-size-bytes (" + minSize
                    + ") cannot be greater than max-manifest-size-bytes (" + maxSize + ")");
        }
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        try {
            Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();
            Snapshot current = icebergTable.currentSnapshot();
            if (current == null) {
                LOG.info("Table {} has no current snapshot, no manifests to rewrite",
                        table.getName());
                return Lists.newArrayList("0", "0");
            }

            // Build predicate for manifest selection
            Predicate<ManifestFile> predicate = buildManifestPredicate();

            // Execute rewrite operation
            boolean clusterByPartition = namedArguments.getBoolean(CLUSTER_BY_PARTITION);
            int scanThreads = namedArguments.getInt(SCAN_THREAD_POOL_SIZE);

            ManifestRewriteExecutor executor = new ManifestRewriteExecutor();
            ManifestRewriteExecutor.Result result = executor.execute(
                    icebergTable,
                    (ExternalTable) table,
                    clusterByPartition,
                    scanThreads,
                    predicate);

            return result.toStringList();
        } catch (Exception e) {
            LOG.error("Failed to rewrite manifests for table: {}", table.getName(), e);
            throw new UserException("Rewrite manifests failed: " + e.getMessage(), e);
        }
    }

    /**
     * Build predicate for selecting manifest files to rewrite
     */
    private Predicate<ManifestFile> buildManifestPredicate() {
        boolean rewriteAll = namedArguments.getBoolean(REWRITE_ALL);
        long minSize = namedArguments.getLong(MIN_MANIFEST_SIZE_BYTES);
        long maxSize = namedArguments.getLong(MAX_MANIFEST_SIZE_BYTES);

        if (rewriteAll) {
            return mf -> true;
        }

        if (minSize == 0 && maxSize == 0) {
            return mf -> true;
        }

        return mf -> {
            long len = mf.length();
            boolean tooSmall = minSize > 0 && len < minSize;
            boolean tooLarge = maxSize > 0 && len > maxSize;
            return tooSmall || tooLarge;
        };
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("rewritten_manifests_count", Type.INT, false,
                        "Number of data manifests rewritten by this command"),
                new Column("total_data_manifests_count", Type.INT, false,
                        "Total number of data manifests before rewrite")
        );
    }

    @Override
    public String getDescription() {
        return "Rewrite Iceberg manifest files to optimize metadata layout";
    }
}
