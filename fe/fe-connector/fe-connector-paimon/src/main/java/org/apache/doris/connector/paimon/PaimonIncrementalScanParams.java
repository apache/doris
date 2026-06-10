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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.DorisConnectorException;

import java.util.HashMap;
import java.util.Map;

/**
 * Validates the raw Doris {@code @incr(...)} window parameters and produces the paimon SDK scan
 * option map that {@code Table.copy(...)} applies for an incremental read.
 *
 * <p>This is a BYTE-FAITHFUL port of legacy
 * {@code org.apache.doris.datasource.paimon.source.PaimonScanNode#validateIncrementalReadParams}
 * (lines 701-878): per design D-043/D-044 (B5b), the ~180-line validation + paimon option-key
 * production MOVES INTO the connector; fe-core (B5b-3) passes only the RAW Doris param map. The two
 * parameter groups &mdash; snapshot-based ({@code startSnapshotId}/{@code endSnapshotId}/
 * {@code incrementalBetweenScanMode}) and timestamp-based ({@code startTimestamp}/
 * {@code endTimestamp}) &mdash; are MUTUALLY EXCLUSIVE. Every validation rule and every error
 * message string is reproduced for parity, EXCEPT the legacy {@code UserException} (fe-core type)
 * is replaced by {@link DorisConnectorException} (the connector cannot import fe-core).
 *
 * <p>BENIGN DIVERGENCE (documented): legacy seeds the result map with
 * {@code paimonScanParams.put(PAIMON_SCAN_SNAPSHOT_ID, null)} and
 * {@code put(PAIMON_SCAN_MODE, null)} (lines 842-843) as defensive RESETS &mdash; it copies these
 * nulls onto a base {@code Table} that may have inherited {@code scan.snapshot-id}/{@code scan.mode}
 * from a prior pin. Here the connector resolves a FRESH {@code Table} per query (no inherited
 * scan.snapshot-id/scan.mode), so the resets are a no-op in EFFECT. Moreover
 * {@code ConnectorMvccSnapshot.Builder.property(...)} rejects null values
 * ({@code Objects.requireNonNull}). So this port emits ONLY the non-null keys
 * ({@code incremental-between} / {@code incremental-between-timestamp} /
 * {@code incremental-between-scan-mode}); stripping the null resets is byte-parity in EFFECT on a
 * freshly-loaded base table.
 */
public final class PaimonIncrementalScanParams {

    // The keys of incremental read params for the Paimon SDK (legacy PaimonScanNode lines 83-87).
    private static final String PAIMON_SCAN_SNAPSHOT_ID = "scan.snapshot-id";
    private static final String PAIMON_SCAN_MODE = "scan.mode";
    private static final String PAIMON_INCREMENTAL_BETWEEN = "incremental-between";
    private static final String PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE = "incremental-between-scan-mode";
    private static final String PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP = "incremental-between-timestamp";

    // The keys of incremental read params for the Doris statement (legacy PaimonScanNode lines 89-93).
    private static final String DORIS_START_SNAPSHOT_ID = "startSnapshotId";
    private static final String DORIS_END_SNAPSHOT_ID = "endSnapshotId";
    private static final String DORIS_START_TIMESTAMP = "startTimestamp";
    private static final String DORIS_END_TIMESTAMP = "endTimestamp";
    private static final String DORIS_INCREMENTAL_BETWEEN_SCAN_MODE = "incrementalBetweenScanMode";

    private PaimonIncrementalScanParams() {
    }

    /**
     * Validates the raw Doris {@code @incr} window {@code params} and returns the paimon SDK option
     * map (the non-null {@code incremental-between*} keys). Byte-faithful to legacy
     * {@code PaimonScanNode.validateIncrementalReadParams}; throws {@link DorisConnectorException}
     * (in place of the legacy {@code UserException}) with the SAME message strings.
     *
     * @param params the raw Doris incremental-read window arguments
     * @return the paimon scan option map (null-valued reset keys stripped &mdash; see class doc)
     */
    public static Map<String, String> validate(Map<String, String> params) {
        // Check if snapshot-based parameters exist
        boolean hasStartSnapshotId = params.containsKey(DORIS_START_SNAPSHOT_ID)
                && params.get(DORIS_START_SNAPSHOT_ID) != null;
        boolean hasEndSnapshotId = params.containsKey(DORIS_END_SNAPSHOT_ID)
                && params.get(DORIS_END_SNAPSHOT_ID) != null;
        boolean hasIncrementalBetweenScanMode = params.containsKey(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE)
                && params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE) != null;

        // Check if timestamp-based parameters exist
        boolean hasStartTimestamp = params.containsKey(DORIS_START_TIMESTAMP)
                && params.get(DORIS_START_TIMESTAMP) != null;
        boolean hasEndTimestamp = params.containsKey(DORIS_END_TIMESTAMP) && params.get(DORIS_END_TIMESTAMP) != null;

        // Check if any snapshot-based parameters are present
        boolean hasSnapshotParams = hasStartSnapshotId || hasEndSnapshotId || hasIncrementalBetweenScanMode;

        // Check if any timestamp-based parameters are present
        boolean hasTimestampParams = hasStartTimestamp || hasEndTimestamp;

        // Rule 2: The two groups are mutually exclusive
        if (hasSnapshotParams && hasTimestampParams) {
            throw new DorisConnectorException(
                    "Cannot specify both snapshot-based parameters"
                            + "(startSnapshotId, endSnapshotId, incrementalBetweenScanMode) "
                            + "and timestamp-based parameters (startTimestamp, endTimestamp) at the same time");
        }

        // Validate snapshot-based parameters group
        if (hasSnapshotParams) {
            // Rule 3.1 & 3.2: DORIS_START_SNAPSHOT_ID is required
            if (!hasStartSnapshotId) {
                throw new DorisConnectorException(
                        "startSnapshotId is required when using snapshot-based incremental read");
            }

            // Rule 3.3: DORIS_INCREMENTAL_BETWEEN_SCAN_MODE can only appear
            // when both start and end snapshot IDs are specified
            if (hasIncrementalBetweenScanMode && (!hasStartSnapshotId || !hasEndSnapshotId)) {
                throw new DorisConnectorException(
                        "incrementalBetweenScanMode can only be specified when"
                                + " both startSnapshotId and endSnapshotId are provided");
            }

            // Validate snapshot ID values
            if (hasStartSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    if (startSId < 0) {
                        throw new DorisConnectorException("startSnapshotId must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new DorisConnectorException("Invalid startSnapshotId format: " + e.getMessage());
                }
            }

            if (hasEndSnapshotId) {
                try {
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (endSId < 0) {
                        throw new DorisConnectorException("endSnapshotId must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new DorisConnectorException("Invalid endSnapshotId format: " + e.getMessage());
                }
            }

            // Check if both snapshot IDs are present and validate their relationship
            if (hasStartSnapshotId && hasEndSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (startSId > endSId) {
                        throw new DorisConnectorException(
                                "startSnapshotId must be less than or equal to endSnapshotId");
                    }
                } catch (NumberFormatException e) {
                    throw new DorisConnectorException("Invalid snapshot ID format: " + e.getMessage());
                }
            }

            // Validate DORIS_INCREMENTAL_BETWEEN_SCAN_MODE
            if (hasIncrementalBetweenScanMode) {
                String scanMode = params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE).toLowerCase();
                if (!scanMode.equals("auto") && !scanMode.equals("diff")
                        && !scanMode.equals("delta") && !scanMode.equals("changelog")) {
                    throw new DorisConnectorException(
                            "incrementalBetweenScanMode must be one of: auto, diff, delta, changelog");
                }
            }
        }

        // Validate timestamp-based parameters group
        if (hasTimestampParams) {
            // Rule 4.1 & 4.2: DORIS_START_TIMESTAMP is required
            if (!hasStartTimestamp) {
                throw new DorisConnectorException(
                        "startTimestamp is required when using timestamp-based incremental read");
            }

            // Validate timestamp values
            if (hasStartTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    if (startTS < 0) {
                        throw new DorisConnectorException("startTimestamp must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new DorisConnectorException("Invalid startTimestamp format: " + e.getMessage());
                }
            }

            if (hasEndTimestamp) {
                try {
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (endTS <= 0) {
                        throw new DorisConnectorException("endTimestamp must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new DorisConnectorException("Invalid endTimestamp format: " + e.getMessage());
                }
            }

            // Check if both timestamps are present and validate their relationship
            if (hasStartTimestamp && hasEndTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (startTS >= endTS) {
                        throw new DorisConnectorException("startTimestamp must be less than endTimestamp");
                    }
                } catch (NumberFormatException e) {
                    throw new DorisConnectorException("Invalid timestamp format: " + e.getMessage());
                }
            }
        }

        // If no incremental parameters are provided at all, that's also invalid in this context
        if (!hasSnapshotParams && !hasTimestampParams) {
            throw new DorisConnectorException(
                    "Invalid paimon incremental read params: at least one valid parameter group must be specified");
        }

        // Fill the result map based on parameter combinations.
        // BENIGN DIVERGENCE (see class doc): legacy seeds PAIMON_SCAN_SNAPSHOT_ID=null and
        // PAIMON_SCAN_MODE=null here (lines 842-843) as defensive RESETS against a base Table that
        // may have inherited a prior scan.snapshot-id/scan.mode. The connector resolves a FRESH Table
        // per query (no inherited keys), so these null resets are a no-op in EFFECT; and
        // ConnectorMvccSnapshot.Builder.property rejects null values. So we DO NOT seed the null keys
        // (stripping them is byte-parity on a freshly-loaded base table) and emit only the non-null
        // incremental-between* keys below.
        Map<String, String> paimonScanParams = new HashMap<>();

        if (hasSnapshotParams) {
            // Legacy re-seeds PAIMON_SCAN_MODE=null here (line 846); stripped (see above).
            if (hasStartSnapshotId && !hasEndSnapshotId) {
                // Only startSnapshotId is specified
                throw new DorisConnectorException(
                        "endSnapshotId is required when using snapshot-based incremental read");
            } else if (hasStartSnapshotId && hasEndSnapshotId) {
                // Both start and end snapshot IDs are specified
                String startSId = params.get(DORIS_START_SNAPSHOT_ID);
                String endSId = params.get(DORIS_END_SNAPSHOT_ID);
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN, startSId + "," + endSId);
            }

            // Add incremental between scan mode if present.
            // GOTCHA (parity): the value is validated lowercase above, but the ORIGINAL-CASE value is
            // emitted (legacy line 859-860 puts params.get(...) verbatim, not the lowercased copy).
            if (hasIncrementalBetweenScanMode) {
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE,
                        params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE));
            }
        }

        if (hasTimestampParams) {
            String startTS = params.get(DORIS_START_TIMESTAMP);
            String endTS = params.get(DORIS_END_TIMESTAMP);

            if (hasStartTimestamp && !hasEndTimestamp) {
                // Only startTimestamp is specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + Long.MAX_VALUE);
            } else if (hasStartTimestamp && hasEndTimestamp) {
                // Both start and end timestamps are specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + endTS);
            }
        }

        return paimonScanParams;
    }
}
