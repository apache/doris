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
 * Sets the current snapshot of an iceberg table to a specific snapshot id or reference (branch / tag).
 * Connector port of legacy {@code IcebergSetCurrentSnapshotAction}. The mutual-exclusion validation
 * ({@code snapshot_id} xor {@code ref}) throws {@link DorisConnectorException} in place of the legacy
 * {@code AnalysisException}, message-identical (T08 byte-parity); the snapshot-not-found check stays
 * <em>inside</em> the try block so it is re-wrapped by the "Failed to set current snapshot to ..." handler,
 * exactly as legacy.
 */
public class IcebergSetCurrentSnapshotAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";
    public static final String REF = "ref";

    public IcebergSetCurrentSnapshotAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("set_current_snapshot", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        // Either snapshot_id or ref must be provided but not both
        namedArguments.registerOptionalArgument(SNAPSHOT_ID,
                "Snapshot ID to set as current",
                null,
                ArgumentParsers.positiveLong(SNAPSHOT_ID));

        namedArguments.registerOptionalArgument(REF,
                "Snapshot Reference (branch or tag) to set as current",
                null,
                ArgumentParsers.nonEmptyString(REF));
    }

    @Override
    protected void validateIcebergAction() {
        // Either snapshot_id or ref must be provided but not both
        Long snapshotId = namedArguments.getLong(SNAPSHOT_ID);
        String ref = namedArguments.getString(REF);

        if (snapshotId == null && ref == null) {
            throw new DorisConnectorException("Either snapshot_id or ref must be provided");
        }

        if (snapshotId != null && ref != null) {
            throw new DorisConnectorException("snapshot_id and ref are mutually exclusive, only one can be provided");
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
        Snapshot previousSnapshot = icebergTable.currentSnapshot();
        Long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : null;

        Long targetSnapshotId = namedArguments.getLong(SNAPSHOT_ID);
        String ref = namedArguments.getString(REF);

        try {
            if (targetSnapshotId != null) {
                Snapshot targetSnapshot = icebergTable.snapshot(targetSnapshotId);
                if (targetSnapshot == null) {
                    throw new DorisConnectorException(
                            "Snapshot " + targetSnapshotId + " not found in table " + icebergTable.name());
                }

                if (previousSnapshot != null && previousSnapshot.snapshotId() == targetSnapshotId) {
                    return Lists.newArrayList(
                            String.valueOf(previousSnapshotId),
                            String.valueOf(targetSnapshotId)
                    );
                }

                icebergTable.manageSnapshots().setCurrentSnapshot(targetSnapshotId).commit();

            } else if (ref != null) {
                Snapshot refSnapshot = icebergTable.snapshot(ref);
                if (refSnapshot == null) {
                    throw new DorisConnectorException("Reference '" + ref + "' not found in table "
                            + icebergTable.name());
                }
                targetSnapshotId = refSnapshot.snapshotId();

                if (previousSnapshot != null && previousSnapshot.snapshotId() == targetSnapshotId) {
                    return Lists.newArrayList(
                            String.valueOf(previousSnapshotId),
                            String.valueOf(targetSnapshotId)
                    );
                }

                icebergTable.manageSnapshots().setCurrentSnapshot(targetSnapshotId).commit();
            }

            return Lists.newArrayList(
                    String.valueOf(previousSnapshotId),
                    String.valueOf(targetSnapshotId)
            );

        } catch (Exception e) {
            String target = targetSnapshotId != null ? "snapshot " + targetSnapshotId : "reference '" + ref + "'";
            throw new DorisConnectorException(
                    "Failed to set current snapshot to " + target + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("previous_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the snapshot that was current before setting the new current snapshot", false, null),
                new ConnectorColumn("current_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the snapshot that is now set as the current snapshot "
                                + "(from snapshot_id parameter or resolved from ref parameter)", false, null));
    }
}
