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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.foundation.util.ArgumentParsers;

import com.google.common.collect.Lists;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;

/**
 * Fast-forwards one branch to the latest snapshot of another. Connector port of legacy
 * {@code IcebergFastForwardAction}. Bug-for-bug preserved: the {@code snapshotBefore} read is null-guarded
 * but the post-commit {@code snapshotAfter} read is not; {@code sourceBranch} is trimmed only in the result
 * row (not before the SDK call); and {@code previous_ref} is the one nullable result column (legacy passed
 * {@code isAllowNull = true}).
 */
public class IcebergFastForwardAction extends BaseIcebergAction {
    public static final String BRANCH = "branch";
    public static final String TO = "to";

    public IcebergFastForwardAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("fast_forward", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register required arguments for branch and to
        namedArguments.registerRequiredArgument(BRANCH,
                "Name of the  branch to fast-forward to",
                ArgumentParsers.nonEmptyString(BRANCH));
        namedArguments.registerRequiredArgument(TO,
                "Target branch  to fast-forward to",
                ArgumentParsers.nonEmptyString(TO));
    }

    @Override
    protected void validateIcebergAction() {
        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
        String sourceBranch = namedArguments.getString(BRANCH);
        String desBranch = namedArguments.getString(TO);

        try {
            Long snapshotBefore =
                    icebergTable.snapshot(sourceBranch) != null ? icebergTable.snapshot(sourceBranch).snapshotId()
                            : null;
            icebergTable.manageSnapshots().fastForwardBranch(sourceBranch, desBranch).commit();
            long snapshotAfter = icebergTable.snapshot(sourceBranch).snapshotId();
            return Lists.newArrayList(
                    sourceBranch.trim(),
                    String.valueOf(snapshotBefore),
                    String.valueOf(snapshotAfter)
            );

        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to fast-forward branch " + sourceBranch + " to snapshot " + desBranch + ": "
                            + e.getMessage(), e);
        }
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("branch_updated", ConnectorType.of("STRING"),
                        "Name of the branch that was fast-forwarded to match the target branch", false, null),
                new ConnectorColumn("previous_ref", ConnectorType.of("BIGINT"),
                        "Snapshot ID that the branch was pointing to before the fast-forward operation", true, null),
                new ConnectorColumn("updated_ref", ConnectorType.of("BIGINT"),
                        "Snapshot ID that the branch is pointing to after the fast-forward operation", false, null));
    }
}
