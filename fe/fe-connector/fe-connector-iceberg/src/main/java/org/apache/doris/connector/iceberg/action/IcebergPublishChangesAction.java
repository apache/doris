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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.foundation.util.ArgumentParsers;

import com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;

/**
 * Publishes a WAP (write-audit-publish) snapshot by cherry-picking the snapshot tagged with a given
 * {@code wap.id} into the current table state. Connector port of legacy {@code IcebergPublishChangesAction}.
 *
 * <p>Bug-for-bug preserved: the result columns are {@code STRING} (not {@code BIGINT} like the other
 * snapshot actions), and a null snapshot id renders as the literal string {@code "null"} (not a SQL NULL);
 * the WAP snapshot is found by a linear scan over {@code snapshots()}.
 */
public class IcebergPublishChangesAction extends BaseIcebergAction {
    public static final String WAP_ID = "wap_id";
    private static final String WAP_ID_PROP = "wap.id";

    public IcebergPublishChangesAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("publish_changes", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerRequiredArgument(WAP_ID,
                "The WAP ID matching the snapshot to publish",
                ArgumentParsers.nonEmptyString(WAP_ID));
    }

    @Override
    protected void validateIcebergAction() {
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
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
            throw new DorisConnectorException("Cannot find snapshot with " + WAP_ID_PROP + " = " + targetWapId);
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

            String previousSnapshotIdString = previousSnapshotId != null ? String.valueOf(previousSnapshotId) : "null";
            String currentSnapshotIdString = currentSnapshotId != null ? String.valueOf(currentSnapshotId) : "null";

            return Lists.newArrayList(
                    previousSnapshotIdString,
                    currentSnapshotIdString
            );

        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to publish changes for wap.id " + targetWapId + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("previous_snapshot_id", ConnectorType.of("STRING"),
                        "ID of the snapshot before the publish operation", false, null),
                new ConnectorColumn("current_snapshot_id", ConnectorType.of("STRING"),
                        "ID of the new snapshot created as a result of the publish operation", false, null));
    }
}
