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
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg rollback to snapshot action implementation.
 * This action rolls back the Iceberg table to a specific snapshot identified by
 * snapshot ID.
 */
public class IcebergRollbackToSnapshotAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";

    public IcebergRollbackToSnapshotAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("rollback_to_snapshot", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register snapshot_id as a required parameter
        namedArguments.registerRequiredArgument(SNAPSHOT_ID,
                "Snapshot ID to rollback to",
                ArgumentParsers.positiveLong(SNAPSHOT_ID));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Iceberg rollback_to_snapshot procedures don't support partitions or where
        // conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        throw new DdlException("Iceberg rollback_to_snapshot procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Rollback Iceberg table to a specific snapshot";
    }
}
