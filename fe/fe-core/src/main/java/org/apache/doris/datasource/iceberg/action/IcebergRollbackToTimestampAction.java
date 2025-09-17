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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg rollback to timestamp action implementation.
 * This action rolls back the Iceberg table to the snapshot that was current
 * at a specific timestamp.
 */
public class IcebergRollbackToTimestampAction extends BaseIcebergAction {
    public static final String TIMESTAMP = "timestamp";

    public IcebergRollbackToTimestampAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("rollback_to_timestamp", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void registerIcebergArguments() {
        // Create a custom timestamp parser that supports both ISO datetime and
        // millisecond formats
        namedArguments.registerRequiredArgument(TIMESTAMP,
                "A timestamp to rollback to (ISO datetime 'yyyy-MM-ddTHH:mm:ss' or milliseconds since epoch)",
                value -> {
                    if (value == null || value.trim().isEmpty()) {
                        throw new IllegalArgumentException("timestamp cannot be empty");
                    }

                    String trimmed = value.trim();

                    // Try to parse as milliseconds first
                    try {
                        long timestampMs = Long.parseLong(trimmed);
                        if (timestampMs < 0) {
                            throw new IllegalArgumentException("Timestamp must be non-negative: " + timestampMs);
                        }
                        return trimmed;
                    } catch (NumberFormatException e) {
                        // If not a number, try as ISO datetime format
                        try {
                            java.time.LocalDateTime.parse(trimmed,
                                    java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                            return trimmed;
                        } catch (java.time.format.DateTimeParseException dte) {
                            throw new IllegalArgumentException("Invalid timestamp format. Expected ISO datetime "
                                    + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: " + trimmed);
                        }
                    }
                });
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Iceberg rollback_to_timestamp procedures don't support partitions or where
        // conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();

        String timestampStr = namedArguments.getString(TIMESTAMP);
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            throw new UserException("timestamp parameter is required for rollback_to_timestamp operation");
        }

        Snapshot previousSnapshot = icebergTable.currentSnapshot();
        long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : 0L;

        long targetTimestamp = TimeUtils.timeStringToLong(timestampStr, TimeUtils.getTimeZone());

        try {
            icebergTable.manageSnapshots().rollbackToTime(targetTimestamp).commit();

            Snapshot currentSnapshot = icebergTable.currentSnapshot();
            long currentSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : 0L;
            // invalid iceberg catalog table cache.
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache((ExternalTable) table);
            return Lists.newArrayList(
                    String.valueOf(previousSnapshotId),
                    String.valueOf(currentSnapshotId)
            );

        } catch (Exception e) {
            throw new UserException("Failed to rollback to timestamp " + timestampStr + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("previous_snapshot_id", Type.BIGINT, false,
                        "ID of the snapshot that was current before the rollback operation"),
                new Column("current_snapshot_id", Type.BIGINT, false,
                        "ID of the snapshot that was current at the specified timestamp and is now set as current"));
    }

    @Override
    public String getDescription() {
        return "Rollback Iceberg table to the snapshot that was current at a specific timestamp";
    }
}
