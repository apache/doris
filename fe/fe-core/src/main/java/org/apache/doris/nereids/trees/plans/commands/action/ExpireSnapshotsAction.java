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
 * Expire snapshots action for Iceberg tables.
 * Removes old snapshots and their associated data files.
 */
public class ExpireSnapshotsAction extends OptimizeAction {
    private static final Logger LOG = LogManager.getLogger(ExpireSnapshotsAction.class);

    // ExpireSnapshots-specific properties
    public static final String OLDER_THAN = "older_than";
    public static final String RETAIN_LAST = "retain_last";
    public static final String MAX_SNAPSHOT_AGE_MS = "max_snapshot_age_ms";
    public static final String MIN_SNAPSHOTS_TO_KEEP = "min_snapshots_to_keep";
    public static final String DELETE_ORPHAN_FILES = "delete_orphan_files";

    public ExpireSnapshotsAction(Map<String, String> properties,
                                Optional<PartitionNamesInfo> partitionNamesInfo,
                                Optional<Expression> whereCondition,
                                ExternalTable table) throws DdlException {
        super(ACTION_EXPIRE_SNAPSHOTS, properties, partitionNamesInfo, whereCondition);
        validateIcebergTable(table);
    }

    @Override
    public void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException {
        validateCommon(tableNameInfo, currentUser);
        validateNoPartitions(); // Expire snapshots operates on entire table
        validateNoWhereCondition(); // WHERE condition not applicable for snapshot expiration

        // Validate that at least one expiration criteria is specified
        String olderThan = properties.get(OLDER_THAN);
        String retainLast = properties.get(RETAIN_LAST);
        String maxSnapshotAge = properties.get(MAX_SNAPSHOT_AGE_MS);
        
        if (olderThan == null && retainLast == null && maxSnapshotAge == null) {
            throw new DdlException("At least one of 'older_than', 'retain_last', or 'max_snapshot_age_ms' must be specified");
        }

        // Validate timestamp format for older_than
        if (olderThan != null) {
            try {
                Long.parseLong(olderThan);
            } catch (NumberFormatException e) {
                throw new DdlException("'older_than' must be a valid timestamp in milliseconds");
            }
        }

        // Validate retain_last
        if (retainLast != null) {
            try {
                int retainCount = Integer.parseInt(retainLast);
                if (retainCount < 1) {
                    throw new DdlException("'retain_last' must be a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("'retain_last' must be a valid integer");
            }
        }

        // Validate max_snapshot_age_ms
        if (maxSnapshotAge != null) {
            try {
                long ageMs = Long.parseLong(maxSnapshotAge);
                if (ageMs <= 0) {
                    throw new DdlException("'max_snapshot_age_ms' must be a positive number");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("'max_snapshot_age_ms' must be a valid number");
            }
        }
    }

    @Override
    public void execute(ExternalTable table) throws UserException {
        IcebergExternalTable icebergTable = (IcebergExternalTable) table;
        
        LOG.info("Executing expire snapshots action for table: {}.{}.{}", 
                icebergTable.getCatalog().getName(), icebergTable.getDbName(), icebergTable.getName());

        try {
            // TODO: Implement actual Iceberg expire snapshots logic
            throw new DdlException("Expire snapshots action implementation is not yet complete");
            
        } catch (Exception e) {
            LOG.error("Failed to execute expire snapshots action for Iceberg table: {}", 
                     icebergTable.getName(), e);
            throw new DdlException("Failed to expire snapshots for Iceberg table: " + e.getMessage());
        }
    }

    @Override
    public boolean isSupported(ExternalTable table) {
        return table instanceof IcebergExternalTable;
    }

    @Override
    public String getDescription() {
        StringBuilder desc = new StringBuilder("Expire snapshots");
        
        String olderThan = properties.get(OLDER_THAN);
        if (olderThan != null) {
            desc.append(" older than ").append(olderThan);
        }
        
        String retainLast = properties.get(RETAIN_LAST);
        if (retainLast != null) {
            desc.append(" keeping last ").append(retainLast).append(" snapshots");
        }
        
        String deleteOrphans = getProperty(DELETE_ORPHAN_FILES, "false");
        if ("true".equalsIgnoreCase(deleteOrphans)) {
            desc.append(" and delete orphan files");
        }
        
        return desc.toString();
    }
}
