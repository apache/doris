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
import org.apache.doris.connector.iceberg.IcebergTimeUtils;

import com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Rolls the iceberg table back to the snapshot current at a given timestamp. Connector port of legacy
 * {@code IcebergRollbackToTimestampAction}.
 *
 * <p><b>Time-zone parity (the one connector-specific change).</b> Legacy parsed the datetime argument with
 * {@code TimeUtils.msTimeStringToLong(str, TimeUtils.getTimeZone())} — the millisecond format
 * {@code yyyy-MM-dd HH:mm:ss.SSS} interpreted in the FE session time zone (read from the thread-local
 * {@code ConnectContext}). The connector cannot reach {@code ConnectContext}, so it reads the session time
 * zone from {@link ConnectorSession} and resolves it through {@link IcebergTimeUtils#resolveSessionZone} (the
 * same Doris alias map, CST -> Asia/Shanghai), then parses via {@link IcebergTimeUtils#msTimeStringToLong}
 * (the millisecond-format, {@code -1}-on-failure mirror of the legacy helper). The argument validator and the
 * {@link #parseTimestampMillis} structure (millis-first, then datetime, then the {@code -1} sentinel error)
 * are otherwise verbatim.
 */
public class IcebergRollbackToTimestampAction extends BaseIcebergAction {
    private static final DateTimeFormatter DATETIME_MS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final String TIMESTAMP = "timestamp";

    public IcebergRollbackToTimestampAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("rollback_to_timestamp", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        // Create a custom timestamp parser that supports both ISO datetime and millisecond formats
        namedArguments.registerRequiredArgument(TIMESTAMP,
                "A timestamp to rollback to (formats: 'yyyy-MM-dd HH:mm:ss.SSS' or milliseconds since epoch)",
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
                        // Second attempt: Parse as ISO datetime format (yyyy-MM-dd HH:mm:ss.SSS)
                        try {
                            java.time.LocalDateTime.parse(trimmed, DATETIME_MS_FORMAT);
                            return trimmed;
                        } catch (java.time.format.DateTimeParseException dte) {
                            throw new IllegalArgumentException("Invalid timestamp format. Expected ISO datetime "
                                    + "(yyyy-MM-dd HH:mm:ss.SSS) or timestamp in milliseconds: " + trimmed);
                        }
                    }
                });
    }

    @Override
    protected void validateIcebergAction() {
        // Iceberg rollback_to_timestamp procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
        String timestampStr = namedArguments.getString(TIMESTAMP);

        Snapshot previousSnapshot = icebergTable.currentSnapshot();
        Long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : null;

        try {
            long targetTimestamp = parseTimestampMillis(timestampStr, IcebergTimeUtils.resolveSessionZone(session));
            icebergTable.manageSnapshots().rollbackToTime(targetTimestamp).commit();

            Snapshot currentSnapshot = icebergTable.currentSnapshot();
            Long currentSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;
            return Lists.newArrayList(
                    String.valueOf(previousSnapshotId),
                    String.valueOf(currentSnapshotId)
            );

        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to rollback to timestamp " + timestampStr + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("previous_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the snapshot that was current before the rollback operation", false, null),
                new ConnectorColumn("current_snapshot_id", ConnectorType.of("BIGINT"),
                        "ID of the snapshot that was current at the specified timestamp and is now set as current",
                        false, null));
    }

    static long parseTimestampMillis(String timestampStr, ZoneId zone) {
        String trimmed = timestampStr.trim();
        try {
            long timestampMs = Long.parseLong(trimmed);
            if (timestampMs < 0) {
                throw new IllegalArgumentException("Timestamp must be non-negative: " + timestampMs);
            }
            return timestampMs;
        } catch (NumberFormatException e) {
            long parsedTimestamp = IcebergTimeUtils.msTimeStringToLong(trimmed, zone);
            if (parsedTimestamp < 0) {
                throw new IllegalArgumentException("Invalid timestamp format. Expected ISO datetime "
                        + "(yyyy-MM-dd HH:mm:ss.SSS) or timestamp in milliseconds: " + trimmed, e);
            }
            return parsedTimestamp;
        }
    }
}
