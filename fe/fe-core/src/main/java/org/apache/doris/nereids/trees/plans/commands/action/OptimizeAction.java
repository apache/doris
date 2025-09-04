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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * Abstract base class for all OPTIMIZE TABLE actions.
 * This class provides a framework for implementing different optimization
 * strategies
 * across various table engines, with special focus on Iceberg procedures.
 */
public abstract class OptimizeAction {
    // Common action types
    public static final String ACTION_CHERRYPICK_SNAPSHOT = "cherrypick_snapshot";
    public static final String ACTION_EXPIRE_SNAPSHOTS = "expire_snapshots";
    public static final String ACTION_FAST_FORWARD = "fast_forward";
    public static final String ACTION_REWRITE_DATA_FILES = "rewrite_data_files";
    public static final String ACTION_ROLLBACK_TO_SNAPSHOT = "rollback_to_snapshot";
    public static final String ACTION_ROLLBACK_TO_TIMESTAMP = "rollback_to_timestamp";
    public static final String ACTION_SET_CURRENT_SNAPSHOT = "set_current_snapshot";

    private static final Logger LOG = LogManager.getLogger(OptimizeAction.class);

    protected final String actionType;
    protected final Map<String, String> properties;
    protected final Optional<PartitionNamesInfo> partitionNamesInfo;
    protected final Optional<Expression> whereCondition;

    protected OptimizeAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        this.actionType = actionType;
        this.properties = properties != null ? properties : Maps.newHashMap();
        this.partitionNamesInfo = partitionNamesInfo;
        this.whereCondition = whereCondition;
    }

    /**
     * Factory method to create specific action instances based on action type and
     * table engine.
     */
    public static OptimizeAction createAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            ExternalTable table) throws DdlException {

        switch (actionType.toLowerCase()) {
            case ACTION_ROLLBACK_TO_SNAPSHOT:
                return new RollbackToSnapshotAction(properties, partitionNamesInfo, whereCondition, table);
            case ACTION_ROLLBACK_TO_TIMESTAMP:
                return new RollbackToTimestampAction(properties, partitionNamesInfo, whereCondition, table);
            case ACTION_SET_CURRENT_SNAPSHOT:
                return new SetCurrentSnapshotAction(properties, partitionNamesInfo, whereCondition, table);
            case ACTION_CHERRYPICK_SNAPSHOT:
                return new CherrypickSnapshotAction(properties, partitionNamesInfo, whereCondition, table);
            case ACTION_FAST_FORWARD:
                return new FastForwardAction(properties, partitionNamesInfo, whereCondition, table);
            default:
                throw new DdlException("Unsupported optimize action: " + actionType);
        }
    }

    /**
     * Validate the action parameters and permissions.
     */
    public abstract void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException;

    /**
     * Execute the optimization action.
     */
    public abstract void execute(ExternalTable table) throws UserException;

    /**
     * Check if this action is supported for the given table engine.
     */
    public abstract boolean isSupported(ExternalTable table);

    /**
     * Get action description for logging and error messages.
     */
    public abstract String getDescription();

    // Getters
    public String getActionType() {
        return actionType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Optional<PartitionNamesInfo> getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    public Optional<Expression> getWhereCondition() {
        return whereCondition;
    }

    /**
     * Common validation logic for all actions.
     */
    protected void validateCommon(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException {
        // Check table access permission
        String catalogName = tableNameInfo.getCtl();
        String dbName = tableNameInfo.getDb();
        String tableName = tableNameInfo.getTbl();

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(currentUser, catalogName, dbName, tableName, PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER",
                    currentUser.getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    catalogName + "." + dbName + "." + tableName);
        }
    }

    /**
     * Get property value with default.
     */
    protected String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    /**
     * Get required property, throw exception if missing.
     */
    protected String getRequiredProperty(String key) throws DdlException {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new DdlException("Missing required property: " + key);
        }
        return value;
    }

    /**
     * Validate that the table is an Iceberg table for Iceberg-specific actions.
     */
    protected void validateIcebergTable(ExternalTable table) throws DdlException {
        if (!(table instanceof IcebergExternalTable)) {
            throw new DdlException(String.format("Action '%s' is only supported for Iceberg tables", actionType));
        }
    }

    /**
     * Check if partitions are specified when they shouldn't be.
     */
    protected void validateNoPartitions() throws DdlException {
        if (partitionNamesInfo.isPresent()) {
            throw new DdlException(String.format("Action '%s' does not support partition specification", actionType));
        }
    }

    /**
     * Check if WHERE condition is specified when it shouldn't be.
     */
    protected void validateNoWhereCondition() throws DdlException {
        if (whereCondition.isPresent()) {
            throw new DdlException(String.format("Action '%s' does not support WHERE condition", actionType));
        }
    }
}
