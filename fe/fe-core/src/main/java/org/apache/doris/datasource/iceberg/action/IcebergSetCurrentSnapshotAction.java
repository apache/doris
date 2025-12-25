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
import org.apache.doris.common.AnalysisException;
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
 * Iceberg set current snapshot action implementation.
 * This action sets the current snapshot of an Iceberg table to a specific
 * snapshot ID or reference (branch or tag).
 */
public class IcebergSetCurrentSnapshotAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";
    public static final String REF = "ref";

    public IcebergSetCurrentSnapshotAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super("set_current_snapshot", properties, partitionNamesInfo, whereCondition);
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
    protected void validateIcebergAction() throws UserException {
        // Either snapshot_id or ref must be provided but not both
        Long snapshotId = namedArguments.getLong(SNAPSHOT_ID);
        String ref = namedArguments.getString(REF);

        if (snapshotId == null && ref == null) {
            throw new AnalysisException("Either snapshot_id or ref must be provided");
        }

        if (snapshotId != null && ref != null) {
            throw new AnalysisException("snapshot_id and ref are mutually exclusive, only one can be provided");
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();

        Snapshot previousSnapshot = icebergTable.currentSnapshot();
        Long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : null;

        Long targetSnapshotId = namedArguments.getLong(SNAPSHOT_ID);
        String ref = namedArguments.getString(REF);

        try {
            if (targetSnapshotId != null) {
                Snapshot targetSnapshot = icebergTable.snapshot(targetSnapshotId);
                if (targetSnapshot == null) {
                    throw new UserException(
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
                    throw new UserException("Reference '" + ref + "' not found in table " + icebergTable.name());
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

            // invalid iceberg catalog table cache.
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache((ExternalTable) table);
            return Lists.newArrayList(
                    String.valueOf(previousSnapshotId),
                    String.valueOf(targetSnapshotId)
            );

        } catch (Exception e) {
            String target = targetSnapshotId != null ? "snapshot " + targetSnapshotId : "reference '" + ref + "'";
            throw new UserException("Failed to set current snapshot to " + target + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("previous_snapshot_id", Type.BIGINT, false,
                        "ID of the snapshot that was current before setting the new current snapshot"),
                new Column("current_snapshot_id", Type.BIGINT, false,
                        "ID of the snapshot that is now set as the current snapshot "
                                + "(from snapshot_id parameter or resolved from ref parameter)"));
    }

    @Override
    public String getDescription() {
        return "Set current snapshot of Iceberg table to a specific snapshot ID or reference";
    }
}
