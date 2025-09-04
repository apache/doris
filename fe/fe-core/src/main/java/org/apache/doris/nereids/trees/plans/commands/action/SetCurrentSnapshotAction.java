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

import java.util.Map;
import java.util.Optional;

/**
 * Set current snapshot action for Iceberg tables.
 */
public class SetCurrentSnapshotAction extends OptimizeAction {
    public static final String SNAPSHOT_ID = "snapshot_id";

    public SetCurrentSnapshotAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            ExternalTable table) throws DdlException {
        super(ACTION_SET_CURRENT_SNAPSHOT, properties, partitionNamesInfo, whereCondition);
        validateIcebergTable(table);
    }

    @Override
    public void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException {
        validateCommon(tableNameInfo, currentUser);
        validateNoPartitions();
        validateNoWhereCondition();

        String snapshotId = getRequiredProperty(SNAPSHOT_ID);
        try {
            Long.parseLong(snapshotId);
        } catch (NumberFormatException e) {
            throw new DdlException("'snapshot_id' must be a valid long integer");
        }
    }

    @Override
    public void execute(ExternalTable table) throws UserException {
        throw new DdlException("Set current snapshot action implementation is not yet complete");
    }

    @Override
    public boolean isSupported(ExternalTable table) {
        return table instanceof IcebergExternalTable;
    }

    @Override
    public String getDescription() {
        return "Set current snapshot to: " + properties.get(SNAPSHOT_ID);
    }
}
