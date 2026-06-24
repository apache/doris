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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import java.util.List;
import java.util.Map;

/**
 * Factory for iceberg {@code ALTER TABLE EXECUTE} procedure bodies, dispatched by
 * {@link org.apache.doris.connector.iceberg.IcebergProcedureOps}.
 *
 * <p>Connector port of legacy {@code datasource/iceberg/action/IcebergExecuteActionFactory}. Two changes
 * from legacy: the always-dead {@code IcebergExternalTable table} parameter is dropped, and the
 * unknown-procedure rejection throws the connector's {@link DorisConnectorException} instead of
 * {@code DdlException} (the message text is kept byte-identical — T08 byte-parity). The factory now
 * builds {@link BaseIcebergAction} (the connector base), receiving the SPI-neutral {@code List<String>}
 * partition names and {@link ConnectorPredicate} where condition rather than the legacy
 * {@code Optional<PartitionNamesInfo>} / nereids {@code Expression}.
 *
 * <p><b>T03 scaffolding.</b> The {@code createAction} switch carries only the faithful default rejection;
 * the 9 procedure cases (their bodies) are ported in T04 ({@code rewrite_data_files} in T05/T06). The
 * {@link #getSupportedActions()} registry — exported to {@code getSupportedProcedures()} and embedded in
 * the rejection message — is complete and final.
 */
public class IcebergExecuteActionFactory {

    // Iceberg procedure names (mapped to action types)
    public static final String ROLLBACK_TO_SNAPSHOT = "rollback_to_snapshot";
    public static final String ROLLBACK_TO_TIMESTAMP = "rollback_to_timestamp";
    public static final String SET_CURRENT_SNAPSHOT = "set_current_snapshot";
    public static final String CHERRYPICK_SNAPSHOT = "cherrypick_snapshot";
    public static final String FAST_FORWARD = "fast_forward";
    public static final String EXPIRE_SNAPSHOTS = "expire_snapshots";
    public static final String REWRITE_DATA_FILES = "rewrite_data_files";
    public static final String PUBLISH_CHANGES = "publish_changes";
    public static final String REWRITE_MANIFESTS = "rewrite_manifests";

    /**
     * Create an iceberg procedure body for {@code actionType}.
     *
     * @param actionType     the procedure name (iceberg procedure / EXECUTE action name)
     * @param properties     the procedure arguments
     * @param partitionNames the {@code PARTITION (...)} names (engine-neutral pass-through)
     * @param whereCondition the engine-lowered {@code WHERE} predicate, or {@code null}
     * @return the procedure body
     * @throws DorisConnectorException if {@code actionType} is not a supported iceberg procedure
     */
    public static BaseIcebergAction createAction(String actionType, Map<String, String> properties,
            List<String> partitionNames, ConnectorPredicate whereCondition) {

        switch (actionType.toLowerCase()) {
            // The 9 procedure cases are ported in T04 (pure-SDK) / T05–T06 (rewrite_data_files):
            //   case ROLLBACK_TO_SNAPSHOT:
            //       return new IcebergRollbackToSnapshotAction(properties, partitionNames, whereCondition);
            //   ...
            default:
                throw new DorisConnectorException("Unsupported Iceberg procedure: " + actionType
                        + ". Supported procedures: " + String.join(", ", getSupportedActions()));
        }
    }

    /**
     * Get supported Iceberg procedure names.
     *
     * @return array of supported procedure names
     */
    public static String[] getSupportedActions() {
        return new String[] {
                ROLLBACK_TO_SNAPSHOT,
                ROLLBACK_TO_TIMESTAMP,
                SET_CURRENT_SNAPSHOT,
                CHERRYPICK_SNAPSHOT,
                FAST_FORWARD,
                EXPIRE_SNAPSHOTS,
                REWRITE_DATA_FILES,
                PUBLISH_CHANGES,
                REWRITE_MANIFESTS
        };
    }
}
