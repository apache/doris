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

/**
 * Represents the information needed to drop a branch in the system.
 *
 */
public class DropBranchInfo {

    private final String branchName;
    private final Boolean ifExists;

    public DropBranchInfo(String branchName, boolean ifExists) {
        this.branchName = branchName;
        this.ifExists = ifExists;
    }

    public String getBranchName() {
        return branchName;
    }

    public Boolean getIfExists() {
        return ifExists;
    }

    /**
     * Generates the SQL representation of the drop branch command.
     *
     * @return SQL string for drop a branch
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP BRANCH");
        if (ifExists) {
            sb.append(" IF EXISTS");
        }
        sb.append(" ").append(branchName);
        return sb.toString();
    }
}
