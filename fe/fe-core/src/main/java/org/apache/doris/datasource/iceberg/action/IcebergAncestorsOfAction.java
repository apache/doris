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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg ancestors_of action implementation.
 * This action reports the live snapshot IDs and timestamps of the ancestors of a specified snapshot.
 * If no snapshot ID is provided, it uses the current snapshot.
 * This procedure helps trace the snapshot lineage and understand the commit history of a table.
 *
 * <p>Use cases:
 * <ul>
 *   <li>Locate rollback targets</li>
 *   <li>Debug commit relationships in multi-branch/multi-tag scenarios</li>
 *   <li>Understand table evolution history</li>
 * </ul>
 *
 * <p>Syntax:
 * <pre>
 * ALTER TABLE table_name EXECUTE ancestors_of();
 * ALTER TABLE table_name EXECUTE ancestors_of("snapshot_id" = "123456789");
 * </pre>
 *
 * <p>Output:
 * <ul>
 *   <li>snapshot_id: The ancestor snapshot ID</li>
 *   <li>timestamp: The snapshot creation timestamp in milliseconds</li>
 * </ul>
 */
public class IcebergAncestorsOfAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";

    public IcebergAncestorsOfAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super("ancestors_of", properties, partitionNamesInfo, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register snapshot_id as an optional parameter
        // If not provided, the current snapshot will be used
        namedArguments.registerOptionalArgument(SNAPSHOT_ID,
                "Snapshot ID to find ancestors for. If not provided, uses the current snapshot.",
                null,
                ArgumentParsers.positiveLong(SNAPSHOT_ID));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // ancestors_of procedure doesn't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    /**
     * Execute the ancestors_of action to return multiple rows (one per ancestor snapshot).
     */
    @Override
    protected List<List<String>> executeAction(TableIf table) throws UserException {
        Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();
        Long targetSnapshotId = namedArguments.getLong(SNAPSHOT_ID);

        try {
            // If no snapshot ID is provided, use the current snapshot
            if (targetSnapshotId == null) {
                Snapshot currentSnapshot = icebergTable.currentSnapshot();
                if (currentSnapshot == null) {
                    // Return empty result if no snapshots exist
                    return new ArrayList<>();
                }
                targetSnapshotId = currentSnapshot.snapshotId();
            } else {
                // Validate that the specified snapshot exists
                Snapshot targetSnapshot = icebergTable.snapshot(targetSnapshotId);
                if (targetSnapshot == null) {
                    throw new UserException(
                            "Snapshot " + targetSnapshotId + " not found in table " + icebergTable.name());
                }
            }

            // Get all ancestor snapshot IDs using Iceberg's SnapshotUtil
            // ancestorIdsBetween returns IDs from targetSnapshotId to the oldest ancestor (null means no limit)
            List<Long> ancestorIds = Lists.newArrayList(
                    SnapshotUtil.ancestorIdsBetween(targetSnapshotId, null, icebergTable::snapshot));

            // Build result rows
            List<List<String>> resultRows = new ArrayList<>();
            for (Long ancestorId : ancestorIds) {
                Snapshot snapshot = icebergTable.snapshot(ancestorId);
                if (snapshot != null) {
                    List<String> row = Lists.newArrayList(
                            String.valueOf(ancestorId),
                            String.valueOf(snapshot.timestampMillis())
                    );
                    resultRows.add(row);
                }
            }

            return resultRows;

        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            throw new UserException("Failed to get ancestors of snapshot " + targetSnapshotId
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("snapshot_id", Type.BIGINT, false,
                        "ID of an ancestor snapshot in the lineage chain, "
                                + "ordered from the specified snapshot to the oldest ancestor"),
                new Column("timestamp", Type.BIGINT, false,
                        "Snapshot creation timestamp in milliseconds"));
    }

    @Override
    public String getDescription() {
        return "Report the ancestor snapshot IDs and timestamps of a specified snapshot or the current snapshot";
    }
}
