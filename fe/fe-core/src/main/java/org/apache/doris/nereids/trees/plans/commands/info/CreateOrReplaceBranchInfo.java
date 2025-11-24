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
 * Represents the information required to create or replace a branch in the Nereids module.
 * <p>
 * This class encapsulates the branch name, operation flags (create, replace, ifNotExists),
 * and associated branch options that control the behavior of the branch operation.
 */
public class CreateOrReplaceBranchInfo {

    private final String branchName;
    private final BranchOptions branchOptions;
    private final Boolean create;
    private final Boolean replace;
    private final Boolean ifNotExists;

    public CreateOrReplaceBranchInfo(String branchName,
                                     boolean create,
                                     boolean replace,
                                     boolean ifNotExists,
                                     BranchOptions branchOptions) {
        this.branchName = branchName;
        this.create = create;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.branchOptions = branchOptions;
    }

    public String getBranchName() {
        return branchName;
    }

    public BranchOptions getBranchOptions() {
        return branchOptions;
    }

    public Boolean getCreate() {
        return create;
    }

    public Boolean getReplace() {
        return replace;
    }

    public Boolean getIfNotExists() {
        return ifNotExists;
    }

    /**
     * Generates the SQL representation of the create or replace branch command.
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (create && replace) {
            sb.append("CREATE OR REPLACE BRANCH");
        } else if (create) {
            sb.append("CREATE BRANCH");
        } else if (replace) {
            sb.append("REPLACE BRANCH");
        }
        if (ifNotExists) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ").append(branchName);
        if (branchOptions != null) {
            sb.append(branchOptions.toSql());
        }
        return sb.toString();
    }
}
