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
 * Implementation for Iceberg cherrypick_snapshot action.
 * This action cherry-picks changes from a snapshot into the current table
 * state.
 * Cherry-picking creates a new snapshot from an existing snapshot without
 * altering
 * or removing the original.
 */
public class IcebergCherrypickSnapshotAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";

    public IcebergCherrypickSnapshotAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("cherrypick_snapshot", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register snapshot_id as a required parameter with type-safe parsing
        namedArguments.registerRequiredArgument(SNAPSHOT_ID,
                "The snapshot ID to cherry-pick",
                ArgumentParsers.positiveLong(SNAPSHOT_ID));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Iceberg cherrypick_snapshot procedures don't support partitions or where
        // conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        throw new DdlException("Iceberg cherrypick_snapshot procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Cherry-pick changes from a specific snapshot in Iceberg table";
    }
}
