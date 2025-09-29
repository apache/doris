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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ResultSet;

import java.util.Map;
import java.util.Optional;

/**
 * Interface for all EXECUTE TABLE actions in Doris.
 * This provides a generic framework for implementing different execution
 * strategies across various table engines (internal tables, external tables,
 * etc.).
 */
public interface ExecuteAction {
    /**
     * Validate the action parameters and permissions.
     *
     * @param tableNameInfo table name information
     * @param currentUser   current user identity
     * @throws UserException if validation fails
     */
    void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException;

    /**
     * Execute the optimization action.
     *
     * @param table the table to operate on
     * @return CommonResultSet if the action returns results, null otherwise
     * @throws UserException if execution fails
     */
    ResultSet execute(TableIf table) throws UserException;

    /**
     * Check if this action is supported for the given table.
     *
     * @param table the table to check
     * @return true if supported, false otherwise
     */
    boolean isSupported(TableIf table);

    /**
     * Get action description for logging and error messages.
     *
     * @return action description
     */
    String getDescription();

    /**
     * Get the action type string.
     *
     * @return action type
     */
    String getActionType();

    /**
     * Get the properties map for this action.
     *
     * @return properties map
     */
    Map<String, String> getProperties();

    /**
     * Get partition names info if specified.
     *
     * @return partition names info
     */
    Optional<PartitionNamesInfo> getPartitionNamesInfo();

    /**
     * Get where condition if specified.
     *
     * @return where condition
     */
    Optional<Expression> getWhereCondition();
}
