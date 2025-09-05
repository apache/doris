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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;

/**
 * Abstract base class for all OPTIMIZE TABLE actions.
 */
public abstract class BaseOptimizeAction implements OptimizeAction {

    protected final String actionType;
    protected final Map<String, String> properties;
    protected final Optional<PartitionNamesInfo> partitionNamesInfo;
    protected final Optional<Expression> whereCondition;

    protected BaseOptimizeAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        this.actionType = actionType;
        this.properties = properties != null ? properties : Maps.newHashMap();
        this.partitionNamesInfo = partitionNamesInfo;
        this.whereCondition = whereCondition;
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

        validateAction();
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
     * Engine-specific validation logic.
     * Subclasses should override this method to implement their specific
     * validation requirements.
     */
    protected abstract void validateAction() throws UserException;

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
            throw new DdlException(String.format("Action '%s' does not support partition specification",
                    actionType));
        }
    }

    /**
     * Check if WHERE condition is specified when it shouldn't be.
     */
    protected void validateNoWhereCondition() throws DdlException {
        if (whereCondition.isPresent()) {
            throw new DdlException(String.format("Action '%s' does not support WHERE condition",
                    actionType));
        }
    }

    /**
     * Check if partitions are required but not specified.
     */
    protected void validateRequiredPartitions() throws DdlException {
        if (!partitionNamesInfo.isPresent()) {
            throw new DdlException(String.format("Action '%s' requires partition specification",
                    actionType));
        }
    }

    /**
     * Check if WHERE condition is required but not specified.
     */
    protected void validateRequiredWhereCondition() throws DdlException {
        if (!whereCondition.isPresent()) {
            throw new DdlException(String.format("Action '%s' requires WHERE condition",
                    actionType));
        }
    }
}
