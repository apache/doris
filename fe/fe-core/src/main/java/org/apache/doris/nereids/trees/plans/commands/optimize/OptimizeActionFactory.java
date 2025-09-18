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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.action.IcebergOptimizeActionFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating OPTIMIZE TABLE actions based on table type.
 * This factory delegates to specific table engine factories to create
 * appropriate action instances, maintaining complete separation between
 * the general OptimizeAction framework and specific table implementations.
 */
public class OptimizeActionFactory {

    /**
     * Create an OptimizeAction instance based on the table type and action type.
     *
     * @param actionType         the type of action to create
     * @param properties         action properties
     * @param partitionNamesInfo partition information
     * @param whereCondition     where condition for filtering
     * @param table              the table to operate on
     * @return OptimizeAction instance
     * @throws DdlException if action creation fails
     */
    public static OptimizeAction createAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            TableIf table) throws DdlException {

        // Delegate to specific table engine factories
        if (table instanceof IcebergExternalTable) {
            return IcebergOptimizeActionFactory.createAction(actionType, properties,
                    partitionNamesInfo, whereCondition, (IcebergExternalTable) table);
        } else if (table instanceof ExternalTable) {
            // Handle other external table types in the future
            throw new DdlException("Optimize actions are not supported for table type: "
                    + table.getClass().getSimpleName());
        } else {
            // Handle internal tables in the future
            // TODO: Implement internal table action factory
            throw new DdlException("Optimize actions for internal tables are not yet supported");
        }
    }

    /**
     * Get available action types for the given table.
     *
     * @param table the table to check
     * @return array of supported action type strings
     */
    public static String[] getSupportedActions(TableIf table) {
        if (table instanceof IcebergExternalTable) {
            return IcebergOptimizeActionFactory.getSupportedActions();
        }
        // Add support for other table types in the future
        return new String[0];
    }
}
