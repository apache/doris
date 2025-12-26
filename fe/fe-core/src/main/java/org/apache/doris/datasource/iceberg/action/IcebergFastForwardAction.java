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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg fast forward action implementation.
 * Fast-forward the current snapshot of one branch to the latest snapshot of
 * another.
 */
public class IcebergFastForwardAction extends BaseIcebergAction {
    public static final String BRANCH = "branch";
    public static final String TO = "to";

    public IcebergFastForwardAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super("fast_forward", properties, partitionNamesInfo, whereCondition);
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
    protected void validateIcebergAction() throws UserException {
        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();

        String sourceBranch = namedArguments.getString(BRANCH);
        String desBranch = namedArguments.getString(TO);

        try {
            Long snapshotBefore =
                    icebergTable.snapshot(sourceBranch) != null ? icebergTable.snapshot(sourceBranch).snapshotId()
                            : null;
            icebergTable.manageSnapshots().fastForwardBranch(sourceBranch, desBranch).commit();
            long snapshotAfter = icebergTable.snapshot(sourceBranch).snapshotId();
            // invalid iceberg catalog table cache.
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache((ExternalTable) table);
            return Lists.newArrayList(
                    sourceBranch.trim(),
                    String.valueOf(snapshotBefore),
                    String.valueOf(snapshotAfter)
            );

        } catch (Exception e) {
            throw new UserException(
                    "Failed to fast-forward branch " + sourceBranch + " to snapshot " + desBranch + ": "
                            + e.getMessage(), e);
        }
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("branch_updated", Type.STRING, false,
                        "Name of the branch that was fast-forwarded to match the target branch"),
                new Column("previous_ref", Type.BIGINT, true,
                        "Snapshot ID that the branch was pointing to before the fast-forward operation"),
                new Column("updated_ref", Type.BIGINT, false,
                        "Snapshot ID that the branch is pointing to after the fast-forward operation"));
    }

    @Override
    public String getDescription() {
        return "Fast-forward the current snapshot of one branch to the latest snapshot of another.";
    }
}
