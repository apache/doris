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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.DropBranchClause;

import java.util.Map;

/**
 * Operation class representing the creation or replacement of a tag in the system.
 * This class extends {@link AlterTableOp} and encapsulates the logic for handling
 * drop branch operations.
 */
public class DropBranchOp extends AlterTableOp {

    private final DropBranchInfo dropBranchInfo;

    public DropBranchOp(String branchName, boolean ifExists) {
        super(AlterOpType.ALTER_BRANCH);
        this.dropBranchInfo = new DropBranchInfo(branchName, ifExists);
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        return dropBranchInfo.toSql();
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new DropBranchClause(dropBranchInfo);
    }
}
