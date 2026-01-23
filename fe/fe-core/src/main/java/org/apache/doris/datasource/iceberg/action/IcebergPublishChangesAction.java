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
 * This action finds a snapshot tagged with a specific 'wap.id' and cherry-picks it
 * into the current table state.
 * Corresponds to Spark syntax: CALL catalog.system.publish_changes('table', 'wap_id_123')
 */
public class IcebergPublishChangesAction extends BaseIcebergAction {
    public static final String WAP_ID = "wap_id";
    private static final String WAP_ID_PROP = "wap.id";

    public IcebergPublishChangesAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo, Optional<Expression> whereCondition) {
        super("publish_changes", properties, partitionNamesInfo, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerRequiredArgument(WAP_ID,
                "The WAP ID matching the snapshot to publish",
                ArgumentParsers.nonEmptyString(WAP_ID));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();
        String targetWapId = namedArguments.getString(WAP_ID);

        // Find the target WAP snapshot
        Snapshot wapSnapshot = null;
        for (Snapshot snapshot : icebergTable.snapshots()) {
            if (targetWapId.equals(snapshot.summary().get(WAP_ID_PROP))) {
                wapSnapshot = snapshot;
                break;
            }
        }

        if (wapSnapshot == null) {
            throw new UserException("Cannot find snapshot with " + WAP_ID_PROP + " = " + targetWapId);
        }

        long wapSnapshotId = wapSnapshot.snapshotId();

        try {
            // Get previous snapshot ID for result
            Snapshot previousSnapshot = icebergTable.currentSnapshot();
            Long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : null;

            // Execute Cherry-pick
            icebergTable.manageSnapshots().cherrypick(wapSnapshotId).commit();

            // Get current snapshot ID after commit
            Snapshot currentSnapshot = icebergTable.currentSnapshot();
            Long currentSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;

            // Invalidate iceberg catalog table cache
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache((ExternalTable) table);

            String previousSnapshotIdString = previousSnapshotId != null ? String.valueOf(previousSnapshotId) : "null";
            String currentSnapshotIdString = currentSnapshotId != null ? String.valueOf(currentSnapshotId) : "null";

            return Lists.newArrayList(
                    previousSnapshotIdString,
                    currentSnapshotIdString
            );

        } catch (Exception e) {
            throw new UserException("Failed to publish changes for wap.id " + targetWapId + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("previous_snapshot_id", Type.STRING, false,
                        "ID of the snapshot before the publish operation"),
                new Column("current_snapshot_id", Type.STRING, false,
                        "ID of the new snapshot created as a result of the publish operation"));
    }

    @Override
    public String getDescription() {
        return "Publish a WAP snapshot by cherry-picking it to the current table state";
    }
}
