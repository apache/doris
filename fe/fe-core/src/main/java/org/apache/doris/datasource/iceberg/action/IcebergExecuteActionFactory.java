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

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache.CatalogGenerationChangedException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteAction;

import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating Iceberg-specific EXECUTE TABLE actions.
 */
public class IcebergExecuteActionFactory {

    // Iceberg procedure names (mapped to action types)
    public static final String ROLLBACK_TO_SNAPSHOT = "rollback_to_snapshot";
    public static final String ROLLBACK_TO_TIMESTAMP = "rollback_to_timestamp";
    public static final String SET_CURRENT_SNAPSHOT = "set_current_snapshot";
    public static final String CHERRYPICK_SNAPSHOT = "cherrypick_snapshot";
    public static final String FAST_FORWARD = "fast_forward";
    public static final String EXPIRE_SNAPSHOTS = "expire_snapshots";
    public static final String REWRITE_DATA_FILES = "rewrite_data_files";
    public static final String PUBLISH_CHANGES = "publish_changes";
    public static final String REWRITE_MANIFESTS = "rewrite_manifests";
    public static final String COMPUTE_PARTITION_STATS = "compute_partition_stats";

    /**
     * Create an Iceberg-specific ExecuteAction instance.
     *
     * @param actionType         the type of action to create (corresponds to
     *                           Iceberg procedure name)
     * @param properties         action properties (will be passed to Iceberg
     *                           procedures)
     * @param partitionNamesInfo partition information
     * @param whereCondition     where condition for filtering
     * @param table              the Iceberg table to operate on
     * @return ExecuteAction instance that wraps Iceberg procedure calls
     * @throws DdlException if action creation fails
     */
    public static ExecuteAction createAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable table) throws DdlException {
        IcebergMetadataOps metadataOps = getMetadataOps(table);

        switch (actionType.toLowerCase()) {
            case ROLLBACK_TO_SNAPSHOT:
                return new IcebergRollbackToSnapshotAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case ROLLBACK_TO_TIMESTAMP:
                return new IcebergRollbackToTimestampAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case SET_CURRENT_SNAPSHOT:
                return new IcebergSetCurrentSnapshotAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case CHERRYPICK_SNAPSHOT:
                return new IcebergCherrypickSnapshotAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case FAST_FORWARD:
                return new IcebergFastForwardAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case EXPIRE_SNAPSHOTS:
                return new IcebergExpireSnapshotsAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case REWRITE_DATA_FILES:
                return new IcebergRewriteDataFilesAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case PUBLISH_CHANGES:
                return new IcebergPublishChangesAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case REWRITE_MANIFESTS:
                return new IcebergRewriteManifestsAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            case COMPUTE_PARTITION_STATS:
                return new IcebergComputePartitionStatsAction(properties, partitionNamesInfo,
                        whereCondition, metadataOps);
            default:
                throw new DdlException("Unsupported Iceberg procedure: " + actionType
                        + ". Supported procedures: " + String.join(", ", getSupportedActions()));
        }
    }

    private static IcebergMetadataOps getMetadataOps(IcebergExternalTable table) throws DdlException {
        ExternalCatalog catalog = table.getCatalog();
        IcebergMetadataOps metadataOps;
        if (catalog instanceof HMSExternalCatalog) {
            metadataOps = ((HMSExternalCatalog) catalog).getIcebergMetadataOps();
        } else if (catalog instanceof IcebergExternalCatalog) {
            metadataOps = (IcebergMetadataOps) catalog.getMetadataOps();
        } else {
            throw new DdlException("Unsupported catalog type for Iceberg execute action: "
                    + catalog.getClass().getSimpleName());
        }
        if (metadataOps == null) {
            throw new CatalogGenerationChangedException(
                    "Catalog was reset while preparing Iceberg execute action");
        }
        return metadataOps;
    }

    /**
     * Get supported Iceberg procedure names.
     *
     * @return array of supported procedure names
     */
    public static String[] getSupportedActions() {
        return new String[] {
                ROLLBACK_TO_SNAPSHOT,
                ROLLBACK_TO_TIMESTAMP,
                SET_CURRENT_SNAPSHOT,
                CHERRYPICK_SNAPSHOT,
                FAST_FORWARD,
                EXPIRE_SNAPSHOTS,
                REWRITE_DATA_FILES,
                PUBLISH_CHANGES,
                REWRITE_MANIFESTS,
                COMPUTE_PARTITION_STATS
        };
    }
}
