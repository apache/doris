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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * REVOKE privilege[(col1,col2...)] [, privilege] ON db.tbl FROM user_identity [ROLE 'role'];
 */
public class RevokeTablePrivilegeCommand extends Command implements ForwardWithSync {
    private final Optional<UserIdentity> userIdentity;
    private final TablePattern tablePattern;
    private final Optional<String> role;
    // AccessPrivileges will be parsed into two parts,
    // with the column permissions section placed in "colPrivileges" and the others in "privileges"
    private final List<AccessPrivilegeWithCols> accessPrivileges;

    private Set<Privilege> privileges = Sets.newHashSet();
    // Privilege,ctl,db,table -> cols
    private Map<ColPrivilegeKey, Set<String>> colPrivileges = Maps.newHashMap();

    public RevokeTablePrivilegeCommand(List<AccessPrivilegeWithCols> accessPrivileges, TablePattern tablePattern,
            Optional<UserIdentity> userIdentity, Optional<String> role) {
        super(PlanType.REVOKE_TABLE_PRIVILEGE_COMMAND);
        this.accessPrivileges = accessPrivileges;
        this.tablePattern = tablePattern;
        this.userIdentity = userIdentity;
        this.role = role;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Env.getCurrentEnv().getAuth().revokeTablePrivilegeCommand(this);
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            throw new AnalysisException("Revoke is prohibited when Ranger is enabled.");
        }

        tablePattern.analyze();

        if (userIdentity.isPresent()) {
            userIdentity.get().analyze();
        } else {
            FeNameFormat.checkRoleName(role.get(), false /* can not be superuser */, "Can not revoke from role");
        }

        if (!CollectionUtils.isEmpty(accessPrivileges)) {
            GrantTablePrivilegeCommand.checkAccessPrivileges(accessPrivileges);

            for (AccessPrivilegeWithCols accessPrivilegeWithCols : accessPrivileges) {
                accessPrivilegeWithCols.transferAccessPrivilegeToDoris(privileges, colPrivileges, tablePattern);
            }
        }

        if (CollectionUtils.isEmpty(privileges) && !role.isPresent() && MapUtils.isEmpty(colPrivileges)) {
            throw new AnalysisException("No privileges or roles in revoke statement.");
        }

        // Revoke operation obey the same rule as Grant operation. reuse the same method
        if (tablePattern != null) {
            GrantTablePrivilegeCommand.checkTablePrivileges(privileges, tablePattern, colPrivileges);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRevokeTablePrivilegeCommand(this, context);
    }

    public Optional<UserIdentity> getUserIdentity() {
        return userIdentity;
    }

    public TablePattern getTablePattern() {
        return tablePattern;
    }

    public Optional<String> getRole() {
        return role;
    }

    public List<AccessPrivilegeWithCols> getAccessPrivileges() {
        return accessPrivileges;
    }

    public Set<Privilege> getPrivileges() {
        return privileges;
    }

    public Map<ColPrivilegeKey, Set<String>> getColPrivileges() {
        return colPrivileges;
    }
}
