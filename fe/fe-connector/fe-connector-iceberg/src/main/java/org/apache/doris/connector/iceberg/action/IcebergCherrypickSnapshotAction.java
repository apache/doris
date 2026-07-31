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
 * Cherry-picks the changes of a snapshot into the current table state. Connector port of legacy
 * {@code IcebergCherrypickSnapshotAction}. Bug-for-bug: the not-found check is <em>inside</em> the try (so it
 * is re-wrapped by the "Failed to cherry-pick snapshot ..." handler) and uses the generic legacy message
 * "Snapshot not found in table" (no id interpolation); the post-commit {@code currentSnapshot()} is read
 * without a null guard, exactly as legacy.
 */
public class IcebergCherrypickSnapshotAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";

    public IcebergCherrypickSnapshotAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("cherrypick_snapshot", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register snapshot_id as a required parameter with type-safe parsing
        namedArguments.registerRequiredArgument(SNAPSHOT_ID,
                "The snapshot ID to cherry-pick",
                ArgumentParsers.positiveLong(SNAPSHOT_ID));
    }

    @Override
    protected void validateIcebergAction() {
        // Iceberg cherrypick_snapshot procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
        Long sourceSnapshotId = namedArguments.getLong(SNAPSHOT_ID);

        try {
            Snapshot targetSnapshot = icebergTable.snapshot(sourceSnapshotId);
            if (targetSnapshot == null) {
                throw new DorisConnectorException("Snapshot not found in table");
            }

            icebergTable.manageSnapshots().cherrypick(sourceSnapshotId).commit();
            Snapshot currentSnapshot = icebergTable.currentSnapshot();

            return Lists.newArrayList(
                    String.valueOf(sourceSnapshotId),
                    String.valueOf(currentSnapshot.snapshotId()
                    )
            );

        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to cherry-pick snapshot " + sourceSnapshotId + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("source_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the snapshot whose changes were cherry-picked into the current table state",
                        false, null),
                new ConnectorColumn("current_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the new snapshot created as a result of the cherry-pick operation, "
                                + "now set as the current snapshot", false, null));
    }
}
