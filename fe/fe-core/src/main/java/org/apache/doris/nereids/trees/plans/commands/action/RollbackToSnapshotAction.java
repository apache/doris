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

package org.apache.doris.nereids.trees.plans.commands.action;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * Rollback to snapshot action for Iceberg tables.
 * Rolls back the table to a specific snapshot ID.
 */
public class RollbackToSnapshotAction extends OptimizeAction {
    public static final String SNAPSHOT_ID = "snapshot_id";

    private static final Logger LOG = LogManager.getLogger(RollbackToSnapshotAction.class);

    public RollbackToSnapshotAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            ExternalTable table) throws DdlException {
        super(ACTION_ROLLBACK_TO_SNAPSHOT, properties, partitionNamesInfo, whereCondition);
        validateIcebergTable(table);
    }

    @Override
    public void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException {
        validateCommon(tableNameInfo, currentUser);
        validateNoPartitions(); // Rollback operates on entire table
        validateNoWhereCondition(); // WHERE condition not applicable for rollback

        // Validate that snapshot_id is specified
        String snapshotId = getRequiredProperty(SNAPSHOT_ID);

        // Validate snapshot ID format
        try {
            Long.parseLong(snapshotId);
        } catch (NumberFormatException e) {
            throw new DdlException("'snapshot_id' must be a valid long integer");
        }
    }

    @Override
    public void execute(ExternalTable table) throws UserException {
        IcebergExternalTable icebergTable = (IcebergExternalTable) table;
        String snapshotId = getRequiredProperty(SNAPSHOT_ID);

        LOG.info("Executing rollback to snapshot action for table: {}.{}.{}, snapshot_id: {}",
                icebergTable.getCatalog().getName(), icebergTable.getDbName(),
                icebergTable.getName(), snapshotId);

        try {
            // TODO: Implement actual Iceberg rollback to snapshot logic
            throw new DdlException("Rollback to snapshot action implementation is not yet complete");

        } catch (Exception e) {
            LOG.error("Failed to execute rollback to snapshot action for Iceberg table: {}",
                    icebergTable.getName(), e);
            throw new DdlException("Failed to rollback Iceberg table to snapshot: " + e.getMessage());
        }
    }

    @Override
    public boolean isSupported(ExternalTable table) {
        return table instanceof IcebergExternalTable;
    }

    @Override
    public String getDescription() {
        String snapshotId = properties.get(SNAPSHOT_ID);
        return "Rollback table to snapshot: " + snapshotId;
    }
}
