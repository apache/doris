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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.action.OptimizeAction;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * Abstract base class for Iceberg-specific OPTIMIZE TABLE actions.
 * This class implements the OptimizeAction interface and provides common
 * functionality for all Iceberg procedures, while being completely decoupled
 * from the general OptimizeAction framework.
 */
public abstract class BaseIcebergAction implements OptimizeAction {
    private static final Logger LOG = LogManager.getLogger(BaseIcebergAction.class);

    protected final String actionType;
    protected final Map<String, String> properties;
    protected final Optional<PartitionNamesInfo> partitionNamesInfo;
    protected final Optional<Expression> whereCondition;
    protected final IcebergExternalTable icebergTable;

    protected BaseIcebergAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        this.actionType = actionType;
        this.properties = properties != null ? properties : Maps.newHashMap();
        this.partitionNamesInfo = partitionNamesInfo;
        this.whereCondition = whereCondition;
        this.icebergTable = icebergTable;
    }

    @Override
    public final void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException {
        // Check table access permissions
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                        tableNameInfo.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER",
                    currentUser.getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getTbl());
        }

        // Delegate to specific Iceberg action validation
        validateIcebergAction();
    }

    @Override
    public final boolean isSupported(TableIf table) {
        return table instanceof IcebergExternalTable;
    }

    @Override
    public String getActionType() {
        return actionType;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Optional<PartitionNamesInfo> getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    @Override
    public Optional<Expression> getWhereCondition() {
        return whereCondition;
    }

    /**
     * Iceberg-specific validation logic.
     * Subclasses should override this method to implement their specific
     * validation.
     */
    protected abstract void validateIcebergAction() throws UserException;

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
     * Check if partitions are specified when they shouldn't be.
     */
    protected void validateNoPartitions() throws DdlException {
        if (partitionNamesInfo.isPresent()) {
            throw new DdlException(String.format("Iceberg procedure '%s' does not support partition specification",
                    actionType));
        }
    }

    /**
     * Check if WHERE condition is specified when it shouldn't be.
     */
    protected void validateNoWhereCondition() throws DdlException {
        if (whereCondition.isPresent()) {
            throw new DdlException(String.format("Iceberg procedure '%s' does not support WHERE condition",
                    actionType));
        }
    }

    /**
     * Get the underlying Iceberg table instance for procedure execution.
     */
    protected IcebergExternalTable getIcebergTable() {
        return icebergTable;
    }
}
