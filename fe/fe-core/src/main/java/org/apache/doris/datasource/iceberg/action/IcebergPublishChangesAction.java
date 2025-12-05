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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implements Iceberg's publish_changes action (Core of the WAP pattern).
 * <p>
 * This action finds a snapshot tagged with a specific 'wap.id' and cherry-picks it
 * into the current table state.
 * <p>
 * Corresponds to Spark syntax: CALL catalog.system.publish_changes('table', 'wap_id_123')
 */
public class IcebergPublishChangesAction extends BaseIcebergAction {

    // Parameter constants
    public static final String WAP_ID = "wap_id";
    // The key used to store wap id in Iceberg metadata (standard Iceberg property)
    private static final String WAP_ID_PROP = "wap.id";

    public IcebergPublishChangesAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super("publish_changes", properties, partitionNamesInfo, whereCondition);
    }

    /**
     * 1. Register arguments.
     * We need a required argument: wap_id (String type).
     */
    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerRequiredArgument(WAP_ID, 
                "The WAP ID matching the snapshot to publish",
                ArgumentParsers.nonEmptyString(WAP_ID));
    }

    /**
     * 2. Validation logic.
     * This action is table-level and does not support partition filtering or WHERE conditions.
     */
    @Override
    protected void validateIcebergAction() throws UserException {
        validateNoPartitions();
        validateNoWhereCondition();
    }

    /**
     * 3. Core execution logic.
     */
    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        // 3.1 Get the underlying Iceberg Table object
        Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();
        String targetWapId = namedArguments.getString(WAP_ID);

        // 3.2 Find the target WAP snapshot
        // Iterate through all snapshots to find the one where summary contains 'wap.id' = targetWapId
        Snapshot wapSnapshot = null;
        for (Snapshot snapshot : icebergTable.snapshots()) {
            String wapIdInSnapshot = snapshot.summary().get(WAP_ID_PROP);
            if (targetWapId.equals(wapIdInSnapshot)) {
                wapSnapshot = snapshot;
                break;
            }
        }

        if (wapSnapshot == null) {
            throw new UserException("Cannot find snapshot with " + WAP_ID_PROP + " = " + targetWapId);
        }

        long wapSnapshotId = wapSnapshot.snapshotId();

        try {
            // 3.3 Record the current snapshot ID before operation (Previous Snapshot ID)
            Snapshot previousSnapshot = icebergTable.currentSnapshot();
            Long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : null;

            // 3.4 Execute Cherry-pick
            // Note: cherry-pick creates a new snapshot containing changes from the wapSnapshot
            icebergTable.manageSnapshots()
                    .cherrypick(wapSnapshotId)
                    .commit();

            // 3.5 Invalidate Doris FE metadata cache
            // Since the table state has changed, we must invalidate the cache to ensure visibility
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache((ExternalTable) table);

            // 3.6 Get the new current snapshot ID after operation (Current Snapshot ID)
            // Must call refresh() to ensure we get the latest state after commit
            icebergTable.refresh();
            Snapshot currentSnapshot = icebergTable.currentSnapshot();
            Long currentSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;

            // 3.7 Return [Previous ID, New ID]
            return Lists.newArrayList(
                    String.valueOf(previousSnapshotId),
                    String.valueOf(currentSnapshotId)
            );

        } catch (Exception e) {
            throw new UserException("Failed to publish changes for wap.id " + targetWapId + ": " + e.getMessage(), e);
        }
    }

    /**
     * 4. Define result schema.
     */
    @Override
    protected List<Column> getResultSchema() {
        // Define result columns matching the return values of executeAction.
        // Use BIGINT type corresponding to Iceberg's snapshot ID.
        return Lists.newArrayList(
                new Column("previous_snapshot_id", Type.BIGINT, false, "The snapshot ID before publishing"),
                new Column("current_snapshot_id", Type.BIGINT, false, "The snapshot ID after publishing")
        );
    }

    @Override
    public String getDescription() {
        return "Publish a WAP snapshot by cherry-picking it to the current table state";
    }
}