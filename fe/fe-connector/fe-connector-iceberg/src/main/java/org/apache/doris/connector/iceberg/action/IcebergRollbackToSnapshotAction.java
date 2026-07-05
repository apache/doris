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
 * Rolls the iceberg table back to a specific snapshot id. Connector port of legacy
 * {@code datasource/iceberg/action/IcebergRollbackToSnapshotAction} — the body is byte-for-byte the legacy
 * SDK call chain with three mechanical changes: it reads the already-loaded SDK {@link Table} (no
 * {@code IcebergExternalTable} downcast), the per-action {@code ExtMetaCacheMgr} cache invalidation moves to
 * dispatch level ({@code IcebergProcedureOps}), and failures throw {@link DorisConnectorException} (unchecked)
 * whose message is kept byte-identical to the legacy {@code UserException} (T08 byte-parity).
 */
public class IcebergRollbackToSnapshotAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";

    public IcebergRollbackToSnapshotAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("rollback_to_snapshot", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register snapshot_id as a required parameter
        namedArguments.registerRequiredArgument(SNAPSHOT_ID,
                "Snapshot ID to rollback to",
                ArgumentParsers.positiveLong(SNAPSHOT_ID));
    }

    @Override
    protected void validateIcebergAction() {
        // Iceberg rollback_to_snapshot procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
        Long targetSnapshotId = namedArguments.getLong(SNAPSHOT_ID);

        Snapshot targetSnapshot = icebergTable.snapshot(targetSnapshotId);
        if (targetSnapshot == null) {
            throw new DorisConnectorException(
                    "Snapshot " + targetSnapshotId + " not found in table " + icebergTable.name());
        }

        try {
            Snapshot previousSnapshot = icebergTable.currentSnapshot();
            Long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : null;
            if (previousSnapshot != null && previousSnapshot.snapshotId() == targetSnapshotId) {
                return Lists.newArrayList(
                        String.valueOf(previousSnapshotId),
                        String.valueOf(targetSnapshotId)
                );
            }
            icebergTable.manageSnapshots().rollbackTo(targetSnapshotId).commit();
            return Lists.newArrayList(
                    String.valueOf(previousSnapshotId),
                    String.valueOf(targetSnapshotId)
            );

        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to rollback to snapshot " + targetSnapshotId + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("previous_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the snapshot that was current before the rollback operation", false, null),
                new ConnectorColumn("current_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the snapshot that is now current after rolling back to the specified snapshot",
                        false, null));
    }
}
