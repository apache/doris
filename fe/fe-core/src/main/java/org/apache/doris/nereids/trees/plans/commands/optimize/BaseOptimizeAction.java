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

package org.apache.doris.nereids.trees.plans.commands.optimize;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.NamedArguments;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
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

    // Named arguments for parameter validation
    protected final NamedArguments namedArguments = new NamedArguments();

    // ResultSet metadata if the action produces results
    protected final ResultSetMetaData resultSetMetaData;

    protected BaseOptimizeAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        this.actionType = actionType;
        this.properties = properties != null ? properties : Maps.newHashMap();
        this.partitionNamesInfo = partitionNamesInfo;
        this.whereCondition = whereCondition;

        // Add OPTIMIZE TABLE specific allowed arguments
        this.namedArguments.addAllowedArgument("action");

        // Register arguments specific to this action
        registerArguments();

        // Initialize ResultSet metadata if the action produces results
        List<Column> resultSchema = getResultSchema();
        if (resultSchema == null || resultSchema.isEmpty()) {
            this.resultSetMetaData = null;
        } else {
            this.resultSetMetaData = new CommonResultSet.CommonResultSetMetaData(resultSchema);
        }
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

        // Validate all registered arguments
        namedArguments.validate(properties);

        // Additional validation logic specific to the action
        validateAction();
    }

    @Override
    public final ResultSet execute(TableIf table) throws UserException {
        List<String> resultRow = executeAction(table);
        if (resultSetMetaData == null || resultRow == null) {
            return null;
        }
        Preconditions.checkState(resultSetMetaData.getColumnCount() == resultRow.size(),
                "Result row size does not match metadata column count");
        // the result should be just one row, so we wrap it in a list
        List<List<String>> resultRows = Lists.newArrayList();
        resultRows.add(resultRow);
        return new CommonResultSet(resultSetMetaData, resultRows);
    }

    /**
     * Register arguments specific to this action type.
     * Subclasses should override this method to register their arguments.
     */
    protected abstract void registerArguments();

    /**
     * Get the result schema if the action produces results.
     * Subclasses can override this method to provide their result schema.
     *
     * @return list of columns representing the result schema
     */
    protected List<Column> getResultSchema() {
        return Lists.newArrayList();
    }

    /**
     * Additional engine-specific validation logic.
     * Subclasses can override this method for custom validation beyond argument
     * validation.
     */
    protected void validateAction() throws UserException {
        // Default implementation does nothing
    }

    /**
     * Execute the action logic.
     * Subclasses must implement this method to perform the actual action.
     *
     * @param table the table to operate on
     * @return list of result if the action produces results, null otherwise
     * @throws UserException if execution fails
     */
    protected abstract List<String> executeAction(TableIf table) throws UserException;

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
     * Check if partitions are required but not specified.
     */
    protected void validateRequiredWhereCondition() throws DdlException {
        if (!whereCondition.isPresent()) {
            throw new DdlException(String.format("Action '%s' requires WHERE condition",
                    actionType));
        }
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
}
