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
 * Iceberg fast forward action implementation.
 * Fast-forward the current snapshot of one branch to the latest snapshot of
 * another.
 */
public class IcebergFastForwardAction extends BaseIcebergAction {
    public static final String BRANCH = "branch";
    public static final String TO = "to";

    public IcebergFastForwardAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("fast_forward", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void registerIcebergArguments() {
        // Register required arguments for branch and to
        namedArguments.registerRequiredArgument(BRANCH,
                "Name of the target branch to fast-forward to",
                ArgumentParsers.nonEmptyString(BRANCH));
        namedArguments.registerRequiredArgument(TO,
                "Target snapshot ID to fast-forward to",
                ArgumentParsers.positiveLong(TO));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        throw new DdlException("Iceberg fast_forward procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Fast-forward the current snapshot of one branch to the latest snapshot of another.";
    }
}
