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
import org.apache.doris.common.AnalysisException;
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
 * Iceberg set current snapshot action implementation.
 * This action sets the current snapshot of an Iceberg table to a specific
 * snapshot ID or reference (branch or tag).
 */
public class IcebergSetCurrentSnapshotAction extends BaseIcebergAction {
    public static final String SNAPSHOT_ID = "snapshot_id";
    public static final String REF = "ref";

    public IcebergSetCurrentSnapshotAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("set_current_snapshot", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void registerIcebergArguments() {
        // Either snapshot_id or ref must be provided but not both
        namedArguments.registerOptionalArgument(SNAPSHOT_ID,
                "Snapshot ID to set as current",
                null,
                ArgumentParsers.positiveLong(SNAPSHOT_ID));

        namedArguments.registerOptionalArgument(REF,
                "Snapshot Reference (branch or tag) to set as current",
                null,
                ArgumentParsers.nonEmptyString(REF));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Either snapshot_id or ref must be provided but not both
        Long snapshotId = namedArguments.getLong(SNAPSHOT_ID);
        String ref = namedArguments.getString(REF);

        if (snapshotId == null && ref == null) {
            throw new AnalysisException("Either snapshot_id or ref must be provided");
        }

        if (snapshotId != null && ref != null) {
            throw new AnalysisException("snapshot_id and ref are mutually exclusive, only one can be provided");
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        throw new DdlException("Iceberg set_current_snapshot procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Set current snapshot of Iceberg table to a specific snapshot ID or reference";
    }
}
