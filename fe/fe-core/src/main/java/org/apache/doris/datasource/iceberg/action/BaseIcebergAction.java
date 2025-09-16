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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.optimize.BaseOptimizeAction;

import java.util.Map;
import java.util.Optional;

/**
 * Abstract base class for Iceberg-specific OPTIMIZE TABLE actions.
 * This class extends BaseOptimizeAction and provides Iceberg-specific
 * functionality while inheriting common optimization action behavior.
 */
public abstract class BaseIcebergAction extends BaseOptimizeAction {

    protected final IcebergExternalTable icebergTable;

    protected BaseIcebergAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super(actionType, properties, partitionNamesInfo, whereCondition);
        this.icebergTable = icebergTable;
    }

    @Override
    public final boolean isSupported(TableIf table) {
        return table instanceof IcebergExternalTable;
    }

    @Override
    protected final void registerArguments() {
        registerIcebergArguments();
    }

    @Override
    protected final void validateAction() throws UserException {
        validateIcebergAction();
    }

    /**
     * Iceberg-specific argument registration.
     * Subclasses should override this method to register their specific
     * arguments.
     */
    protected abstract void registerIcebergArguments();

    /**
     * Iceberg-specific validation logic.
     * Subclasses should override this method to implement their specific
     * validation.
     */
    protected void validateIcebergAction() throws UserException {
        // Default implementation does nothing.
    }

    /**
     * Get the underlying Iceberg table instance for procedure execution.
     */
    protected IcebergExternalTable getIcebergTable() {
        return icebergTable;
    }
}
